#pragma once
#include <condition_variable>
#include <functional>
#include <string>
#include <cstdint>
#include <mutex>
#include <queue>

namespace utils
{
    using Handler = std::function<void(const std::string&, const std::string&)>;
    using SubscriptionId = uint64_t;

    enum class EventBusState : uint8_t { Running, Stopping, Stopped };

    struct Message
    {
        std::string topic;
        std::string payload;
    };

    struct Subscriber
    {
        SubscriptionId m_id{0};
        std::string    m_topic;
        Handler        m_handler;

        std::mutex              m_subMtx;
        std::condition_variable m_subCv;

        std::queue<Message>     m_subMessageFeed;
        bool                    m_isStopped{false};
        bool                    m_isBusy{false};
    };

    struct MonitoringSnapshot
    {
        EventBusState m_state{EventBusState::Running};
        std::unordered_map<std::string, std::size_t>   m_subscribersPerTopic;
        std::unordered_map<std::string, std::size_t> m_publishedPerTopic;
        std::unordered_map<std::string, std::size_t> m_deliveredPerTopic;
        std::size_t m_totalPublished = 0;
        std::size_t m_totalDelivered = 0;
    };

    class NonCopyable
    {
    public:
        NonCopyable() = default;
        NonCopyable(const NonCopyable&) = delete;
        NonCopyable& operator=(const NonCopyable&) = delete;

        NonCopyable(NonCopyable&&) = default;
        NonCopyable& operator=(NonCopyable&&) = default;
    };
} // namespace utils
