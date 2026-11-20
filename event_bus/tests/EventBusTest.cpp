#include "../src/EventBus.h"

#include <gtest/gtest.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <iostream>
#include <mutex>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

using namespace std::chrono_literals;
using bus::EventBus;
using utils::EventBusState;
using utils::SubscriptionId;

class EventBusTest : public testing::Test
{
protected:
    void SetUp() override
    {
        m_bus = std::make_unique<EventBus>(4);
    }

    void TearDown() override
    {
        if (m_bus) m_bus->Shutdown();
    }

    std::unique_ptr<EventBus> m_bus;
};

TEST_F(EventBusTest, TestScenario)
{
    struct LogEntry {
        double t;
        std::string who;
        std::string payload;
    };

    std::mutex logMtx;
    std::vector<LogEntry> journal;

    const auto t0= std::chrono::steady_clock::now();

    auto now = [&] {
        return std::chrono::duration<double>(
            std::chrono::steady_clock::now() - t0).count();
    };

    auto writeLog = [&](const std::string& who, const std::string& payload) {
        std::lock_guard lock(logMtx);
        journal.push_back({now(), who, payload});
    };

    std::atomic<int> hA{0}, hB{0}, hC{0};

    [[maybe_unused]] const auto idA = m_bus->Subscribe("robot.state",
        [&](const std::string&, const std::string& p) { ++hA; writeLog("A", p); });
    const auto idB = m_bus->Subscribe("robot.state",
        [&](const std::string&, const std::string& p) { ++hB; writeLog("B", p); });
    [[maybe_unused]] const auto idC = m_bus->Subscribe("robot.error",
        [&](const std::string&, const std::string& p) { ++hC; writeLog("C", p); });

    constexpr auto kTotalPublish = 5s;
    constexpr auto kUnsubAt = 3s;
    constexpr auto kInterval = 50ms;

    std::atomic stopPubs{false};
    const auto tPubStart = std::chrono::steady_clock::now();

    auto shouldKeepPublishing = [&] {
        return !stopPubs.load(std::memory_order_relaxed) &&
               std::chrono::steady_clock::now() - tPubStart < kTotalPublish;
    };

    std::thread pub1([&] {
        int seq = 0;
        while (shouldKeepPublishing()) {
            try {
                m_bus->Publish("robot.state",
                    "state=Idle;mode=Auto;src=P1;seq=" + std::to_string(seq++));
            } catch (const std::exception&) { break; }
            std::this_thread::sleep_for(kInterval);
        }
    });

    std::thread pub2([&] {
        int seq = 0;
        while (shouldKeepPublishing()) {
            try {
                m_bus->Publish("robot.state",
                    "state=Moving;mode=Auto;src=P2;seq=" + std::to_string(seq));
                if (seq % 10 == 0) {
                    m_bus->Publish("robot.error",
                        "code=42;msg=joint_limit;seq=" + std::to_string(seq));
                }
            } catch (const std::exception&) { break; }
            ++seq;
            std::this_thread::sleep_for(kInterval);
        }
    });


    std::this_thread::sleep_until(tPubStart + kUnsubAt);
    m_bus->Unsubscribe(idB);

    const double tUnsubReturned = now();
    const int hBAtUnsub = hB.load();

    std::this_thread::sleep_until(tPubStart + kTotalPublish);

    stopPubs = true;
    pub1.join();
    pub2.join();

    m_bus->Shutdown();

    int bAfterUnsub = 0;
    double tLastB = -1.0;
    {
        std::lock_guard lock(logMtx);
        for (const auto& e : journal) {
            if (e.who != "B") continue;
            tLastB = std::max(tLastB, e.t);
            if (e.t > tUnsubReturned) ++bAfterUnsub;
        }
        std::cout << "Jornal entries=" << journal.size()
                  << "  A=" << hA.load()
                  << "  B=" << hB.load()
                  << "  C=" << hC.load() << "\n";

        std::cout << "Jornal last B call at t=" << tLastB
                  << "s; unsubscribe returned at t=" << tUnsubReturned << "s\n";

        std::cout << "Jornal last B entries:\n";
        int printed = 0;
        for (auto it = journal.rbegin(); it != journal.rend() && printed < 5; ++it) {
            if (it->who != "B") continue;
            std::cout << "  t=" << it->t << "s  B  " << it->payload << "\n";
            ++printed;
        }
    }

    EXPECT_GT(hA.load(), 0) << "A never received robot.state";
    EXPECT_GT(hC.load(), 0) << "C never received robot.error";
    EXPECT_GT(hBAtUnsub, 0) << "B never received anything before unsubscribe";

    EXPECT_EQ(hB.load(), hBAtUnsub)
        << "B was called " << (hB.load() - hBAtUnsub)
        << " times AFTER Unsubscribe returned";

    EXPECT_EQ(bAfterUnsub, 0)
        << "journal contains " << bAfterUnsub
        << " B entries with t > unsubscribe time";

    EXPECT_EQ(m_bus->Monitor().m_state, EventBusState::Stopped);
}