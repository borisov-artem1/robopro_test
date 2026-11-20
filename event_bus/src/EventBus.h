#pragma once

#include "ThreadPool.h"
#include "../iface/IEventBus.h"
#include "../utils/utils.h"

namespace bus
{
    class EventBus final : public IEventBus
    {
    public:
        explicit EventBus(std::size_t amountOfThreads = 0);
        ~EventBus();

        utils::SubscriptionId Subscribe(const std::string& topic, utils::Handler handler) override;
        void Unsubscribe(utils::SubscriptionId subId) override;
        void Publish(const std::string& topic, const std::string& payload) override;
        void Shutdown() override;
        utils::MonitoringSnapshot Monitor() override;

    private:
        void IncrementDelivered(const std::string& topic);
        void Drain(std::shared_ptr<utils::Subscriber> sub);

        std::unordered_map<std::string, std::vector<std::shared_ptr<utils::Subscriber>>> m_subByTopic;
        std::unordered_map<utils::SubscriptionId, std::shared_ptr<utils::Subscriber>>    m_subById;

        tpool::ThreadPool                  m_pool;

        utils::SubscriptionId              m_nextId{0};
        std::atomic<utils::EventBusState>  m_busState;
        std::atomic<std::size_t>           m_totalPublished;
        std::atomic<std::size_t>           m_totalDelivered;
        std::mutex                         m_busMtx;

        std::mutex                                   m_statMtx;
        std::unordered_map<std::string, std::size_t> m_publishedPerTopic;
        std::unordered_map<std::string, std::size_t> m_deliveredPerTopic;

        int m_drainBudget = 64;

        std::mutex              m_stateMtx;
        std::condition_variable m_stateGuardCv;
    };
} // namespace bus
