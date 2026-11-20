#pragma once
#include <string>
#include "../utils/utils.h"

namespace bus
{
    class IEventBus : public utils::NonCopyable
    {
    public:
        virtual utils::SubscriptionId Subscribe(const std::string& topic, utils::Handler handler) = 0;
        virtual void Unsubscribe(utils::SubscriptionId subId) = 0;
        virtual void Publish(const std::string& topic, const std::string& payload) = 0;
        virtual void Shutdown() = 0;
        virtual utils::MonitoringSnapshot Monitor() = 0;
    };
} // namespace bus
