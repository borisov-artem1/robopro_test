#include "EventBus.h"

#include <string>
#include <functional>
#include <cstdint>
#include <iostream>
#include <gtest/gtest-message.h>

namespace bus
{
    EventBus::EventBus(const std::size_t amountOfThreads)
        : m_pool(amountOfThreads) {}

    EventBus::~EventBus()
    {
        Shutdown();
    }

    utils::SubscriptionId EventBus::Subscribe(const std::string& topic, utils::Handler handler)
    {
        if (!handler)
            throw std::runtime_error("Error in subscribe: Handler not found");

        if (m_busState.load(std::memory_order_relaxed) != utils::EventBusState::Running)
        {
            throw std::runtime_error("Error in subscribe: EventBus is in stopping or stopped state");
        }

        auto sub = std::make_shared<utils::Subscriber>();
        sub->m_topic = topic;
        sub->m_handler = std::move(handler);

        std::lock_guard subLock(m_busMtx);

        if (m_busState.load(std::memory_order_relaxed) != utils::EventBusState::Running)
        {
            throw std::runtime_error("Error in subscribe: EventBus is in stopping or stopped state");
        }

        sub->m_id = m_nextId++;
        m_subByTopic[topic].push_back(sub);
        m_subById[sub->m_id] = sub;

        return sub->m_id;
    }

    void EventBus::Unsubscribe(utils::SubscriptionId subId)
    {
        if (m_busState.load(std::memory_order_relaxed) != utils::EventBusState::Running)
            throw std::runtime_error("Error in unsubscribe: Event bus is in stopping or stopped state");

        std::unique_lock busLock(m_busMtx);

        auto it = m_subById.find(subId);

        if (it == m_subById.end())
        {
            std::cerr << "Subscriber not found" << std::endl;
            return;
        }

        auto sub = it->second;
        m_subById.erase(subId);

        auto topicIt = m_subByTopic.find(sub->m_topic);

        if (topicIt != m_subByTopic.end())
        {
            auto& subsForTopic = topicIt->second;
            subsForTopic.erase(std::remove(subsForTopic.begin(), subsForTopic.end(), sub), subsForTopic.end());
            if (subsForTopic.empty())
            {
                m_subByTopic.erase(topicIt);
            }
        }

        busLock.unlock();

        std::unique_lock subLock(sub->m_subMtx);
        sub->m_isStopped = true;

        if (sub->m_isBusy)
        {
            sub->m_subCv.wait(subLock, [&] { return !sub->m_isBusy; });
        }
    }

    void EventBus::Publish(const std::string& topic, const std::string& payload)
    {
        if (m_busState.load(std::memory_order_relaxed) != utils::EventBusState::Running)
            throw std::runtime_error("Event bus is in stopping or stopped state");

        if (topic.empty())
            throw std::runtime_error("Error while publishing the topic");

        std::vector<std::shared_ptr<utils::Subscriber>> subs;
        {
            std::lock_guard busLock(m_busMtx);
            auto it = m_subByTopic.find(topic);

            if (it != m_subByTopic.end())
                subs = it->second;
        }

        {
            std::lock_guard statLock(m_statMtx);
            m_publishedPerTopic[topic]++;
        }

        m_totalPublished.fetch_add(1, std::memory_order_relaxed);

        utils::Message msg {topic, payload};
        for (auto& sub : subs)
        {
            bool needPost = false;
            {
                std::lock_guard subLock(sub->m_subMtx);
                if (sub->m_isStopped) continue;

                sub->m_subMessageFeed.push(msg);
                if (!sub->m_isBusy) { sub->m_isBusy = true; needPost = true; }
            }

            if (needPost)
            {
                m_pool.Post([sub, this] { Drain(sub); });
            }
        }
    }

    void EventBus::Shutdown()
    {
        auto expected = utils::EventBusState::Running;
        if (!m_busState.compare_exchange_strong(expected, utils::EventBusState::Stopping)) {
            std::unique_lock busLock(m_busMtx);
            m_stateGuardCv.wait(busLock, [&]
            {
                return m_busState.load() == utils::EventBusState::Stopped;
            });
            return;
        }

        std::vector<std::shared_ptr<utils::Subscriber>> all;
        {
            std::lock_guard lock(m_busMtx);
            all.reserve(m_subById.size());
            for (auto& kv : m_subById) all.push_back(kv.second);
        }

        for (const auto& sub : all)
        {
            std::lock_guard subLock(sub->m_subMtx);
            sub->m_isStopped = true;
        }

        for (auto& sub : all) {
            std::unique_lock subLock(sub->m_subMtx);
            sub->m_subCv.wait(subLock, [&] { return !sub->m_isBusy; });
        }

        m_pool.Stop();

        {
            std::lock_guard busLock(m_busMtx);
            m_subByTopic.clear();
            m_subById.clear();
        }

        {
            std::lock_guard stateLock(m_stateMtx);
            m_busState.store(utils::EventBusState::Stopped);
        }
        m_stateGuardCv.notify_all();
    }

    void EventBus::IncrementDelivered(const std::string& topic)
    {
        std::unique_lock busLock(m_statMtx);
        m_deliveredPerTopic[topic]++;

        busLock.unlock();
        m_totalDelivered.fetch_add(1, std::memory_order_relaxed);
    }

    void EventBus::Drain(std::shared_ptr<utils::Subscriber> sub)
    {
        for (int i = 0; i < m_drainBudget; i++)
        {
            utils::Message msg;
            {
                std::lock_guard lock(sub->m_subMtx);
                if (sub->m_subMessageFeed.empty())
                {
                    sub->m_isBusy = false;
                    sub->m_subCv.notify_all();
                    return;
                }
                msg = std::move(sub->m_subMessageFeed.front());
                sub->m_subMessageFeed.pop();
            }

            try
            {
                sub->m_handler(msg.topic, msg.payload);
            } catch (const std::exception& exc)
            {
                std::cerr << "Error while draining the subscriber: " << exc.what() << std::endl;
            }

            IncrementDelivered(msg.topic);
        }

        bool needMore = false;

        {
            std::lock_guard subLock(sub->m_subMtx);

            if (sub->m_subMessageFeed.empty())
            {
                sub->m_isBusy = false;
                sub->m_subCv.notify_all();
                return;
            }

            needMore = true;
        }

        if (needMore)
        {
            m_pool.Post([this, sub] { Drain(sub); });
        }
    }

    utils::MonitoringSnapshot EventBus::Monitor()
    {
        utils::MonitoringSnapshot snap;
        snap.m_state = m_busState.load();

        {
            std::lock_guard lock(m_busMtx);
            for (auto& kv : m_subByTopic)
            {
                if (!kv.second.empty())
                {
                    snap.m_subscribersPerTopic[kv.first] = kv.second.size();
                }
            }
        }
        {
            std::lock_guard statLock(m_statMtx);
            snap.m_publishedPerTopic = m_publishedPerTopic;
            snap.m_deliveredPerTopic = m_deliveredPerTopic;
        }
        snap.m_totalPublished = m_totalPublished.load(std::memory_order_relaxed);
        snap.m_totalDelivered = m_totalDelivered.load(std::memory_order_relaxed);
        return snap;
    }

} // namespace bus
