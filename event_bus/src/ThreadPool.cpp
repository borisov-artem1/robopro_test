#include "ThreadPool.h"
#include <iostream>

namespace tpool
{
    ThreadPool::ThreadPool(std::size_t threadAmount)
    {
        if (threadAmount == 0)
        {
            threadAmount = std::max<std::size_t>(4, std::thread::hardware_concurrency());
        }

        m_workers.reserve(threadAmount);

        for (std::size_t i = 0; i < threadAmount; i++)
        {
            m_workers.emplace_back([this] { worker(); });
        }
    }

    ThreadPool::~ThreadPool()
    {
        Stop();
    }


    void ThreadPool::Post(std::function<void()> task)
    {
        {
            std::lock_guard lock(m_mtx);
            if (!isRunning) return;

            m_taskQueue.push(std::move(task));
        }
        m_guardCv.notify_one();
    }

    void ThreadPool::Stop()
    {
        {
            std::lock_guard lock(m_mtx);
            isRunning = false;
        }

        m_guardCv.notify_all();

        for (auto& worker : m_workers)
        {
            if (worker.joinable())
            {
                worker.join();
            }
        }
    }

    void ThreadPool::worker()
    {
        while (true)
        {
            std::function<void()> task;
            {
                std::unique_lock lock(m_mtx);
                m_guardCv.wait(lock, [this]
                {
                    return !m_taskQueue.empty() || !isRunning;
                });

                if (!isRunning) return;

                task = std::move(m_taskQueue.front());
                m_taskQueue.pop();
            }

            try
            {
                task();
            } catch (std::exception& exc)
            {
                std::cerr << "Event bus thread pool throw exception: " << exc.what() << std::endl;
            }
        }
    }
} // namespace tpool