#pragma once
#include "../iface/IThreadPool.h"

namespace tpool
{
    class ThreadPool final : public IThreadPool
    {
    public:
        explicit ThreadPool(std::size_t threadAmount);
        void Post(std::function<void()>) override;
        void Stop() override;
        ~ThreadPool();

    private:
        void worker() override;

        std::queue<std::function<void()>> m_taskQueue;
        std::vector<std::thread> m_workers;
        std::condition_variable m_guardCv;
        std::mutex m_mtx;
        bool isRunning{true};
    };
} // namespace tpool
