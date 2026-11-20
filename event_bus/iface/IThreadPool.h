#pragma once
#include "../utils/utils.h"

namespace tpool
{
    class IThreadPool : public utils::NonCopyable
    {
    public:
        virtual void Post(std::function<void()>) = 0;
        virtual void Stop() = 0;
    private:
        virtual void worker() = 0;
    };
} // namespace tpool
