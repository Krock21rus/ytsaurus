#include "io_engine.h"
#include "io_engine_base.h"
#include "io_engine_uring.h"
#include "io_request_slicer.h"
#include "private.h"

#include <yt/yt/core/concurrency/action_queue.h>
#include <yt/yt/core/concurrency/two_level_fair_share_thread_pool.h>
#include <yt/yt/core/concurrency/new_fair_share_thread_pool.h>
#include <yt/yt/core/concurrency/thread_pool.h>

#include <yt/yt/core/threading/thread.h>

#include <yt/yt/core/ytree/yson_struct.h>

#include <yt/yt/core/misc/fs.h>
#include <yt/yt/core/misc/sync_cache.h>

#include <yt/yt/client/misc/workload.h>

#include <library/cpp/yt/threading/notification_handle.h>

#include <library/cpp/yt/memory/atomic_intrusive_ptr.h>

#include <library/cpp/yt/system/handle_eintr.h>

#include <util/generic/size_literals.h>
#include <util/generic/xrange.h>
#include <util/generic/set.h>

#include <array>

#ifdef _linux_
    #include <sys/uio.h>
#endif

namespace NYT::NIO {

using namespace NConcurrency;
using namespace NProfiling;

////////////////////////////////////////////////////////////////////////////////

TIOEngineHandle::TIOEngineHandle(const TString& fName, EOpenMode oMode) noexcept
    : TFileHandle(fName, oMode)
    , OpenForDirectIO_(oMode & DirectAligned)
{ }

bool TIOEngineHandle::IsOpenForDirectIO() const
{
     return OpenForDirectIO_;
}

void TIOEngineHandle::MarkOpenForDirectIO(EOpenMode* oMode)
{
    *oMode |= DirectAligned;
}

////////////////////////////////////////////////////////////////////////////////

TFuture<TSharedRef> IIOEngine::ReadAll(
    const TString& path,
    EWorkloadCategory category,
    TSessionId sessionId)
{
    return Open({path, OpenExisting | RdOnly | Seq | CloseOnExec}, category)
        .Apply(BIND([=, this, this_ = MakeStrong(this)] (const TIOEngineHandlePtr& handle) {
            struct TReadAllBufferTag
            { };
            return Read(
                {{handle, 0, handle->GetLength()}},
                category,
                GetRefCountedTypeCookie<TReadAllBufferTag>(),
                sessionId)
                .Apply(BIND(
                    [=, this, this_ = MakeStrong(this), handle = handle]
                    (const TReadResponse& response)
                {
                    YT_VERIFY(response.OutputBuffers.size() == 1);
                    return Close({.Handle = handle}, category)
                        .Apply(BIND([buffers = response.OutputBuffers] {
                            return buffers[0];
                        }));
                }));
        }));
}

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TThreadPoolIOEngineConfig)

class TThreadPoolIOEngineConfig
    : public TIOEngineConfigBase
{
public:
    int ReadThreadCount;
    int WriteThreadCount;

    bool EnablePwritev;
    bool FlushAfterWrite;
    bool AsyncFlushAfterWrite;

    // Request size in bytes.
    i64 DesiredRequestSize;
    i64 MinRequestSize;

    // Fair-share thread pool settings.
    double DefaultPoolWeight;
    double UserInteractivePoolWeight;

    // Two level fair-share thread pool settings.
    THashMap<TString, double> PoolWeights;
    TDuration BucketTtl;

    REGISTER_YSON_STRUCT(TThreadPoolIOEngineConfig);

    static void Register(TRegistrar registrar)
    {
        registrar.Parameter("read_thread_count", &TThis::ReadThreadCount)
            .GreaterThanOrEqual(1)
            .Default(1);
        registrar.Parameter("write_thread_count", &TThis::WriteThreadCount)
            .GreaterThanOrEqual(1)
            .Default(1);

        registrar.Parameter("enable_pwritev", &TThis::EnablePwritev)
            .Default(true);
        registrar.Parameter("flush_after_write", &TThis::FlushAfterWrite)
            .Default(false);
        registrar.Parameter("async_flush_after_write", &TThis::AsyncFlushAfterWrite)
            .Default(false);

        registrar.Parameter("desired_request_size", &TThis::DesiredRequestSize)
            .GreaterThanOrEqual(4_KB)
            .Default(128_KB);
        registrar.Parameter("min_request_size", &TThis::MinRequestSize)
            .GreaterThanOrEqual(512)
            .Default(64_KB);

        registrar.Parameter("default_pool_weight", &TThis::DefaultPoolWeight)
            .GreaterThan(0)
            .Default(1);
        registrar.Parameter("user_interactive_pool_weight", &TThis::UserInteractivePoolWeight)
            .GreaterThanOrEqual(1)
            .Default(4);

        registrar.Parameter("pool_weights", &TThis::PoolWeights)
            .Default();
        registrar.Parameter("bucket_ttl", &TThis::BucketTtl)
            .Default(TDuration::Minutes(5));
    }
};

DEFINE_REFCOUNTED_TYPE(TThreadPoolIOEngineConfig)

////////////////////////////////////////////////////////////////////////////////

class TFixedPriorityExecutor
{
public:
    TFixedPriorityExecutor(
        const TThreadPoolIOEngineConfigPtr& config,
        const TString& locationId,
        NLogging::TLogger /* logger */)
        : ReadThreadPool_(CreateThreadPool(config->ReadThreadCount, Format("IOR:%v", locationId)))
        , WriteThreadPool_(CreateThreadPool(config->WriteThreadCount, Format("IOW:%v", locationId)))
        , ReadInvoker_(CreatePrioritizedInvoker(ReadThreadPool_->GetInvoker(), NProfiling::TTagSet({{"invoker", "fixed_priority_executor_reader"}, {"location_id", locationId}})))
        , WriteInvoker_(CreatePrioritizedInvoker(WriteThreadPool_->GetInvoker(), NProfiling::TTagSet({{"invoker", "fixed_priority_executor_writer"}, {"location_id", locationId}})))
    { }

    IInvokerPtr GetReadInvoker(EWorkloadCategory category, TIOEngineBase::TSessionId)
    {
        return CreateFixedPriorityInvoker(ReadInvoker_, GetBasicPriority(category));
    }

    IInvokerPtr GetWriteInvoker(EWorkloadCategory category, TIOEngineBase::TSessionId)
    {
        return CreateFixedPriorityInvoker(WriteInvoker_, GetBasicPriority(category));
    }

    void Reconfigure(const TThreadPoolIOEngineConfigPtr& config)
    {
        ReadThreadPool_->Configure(config->ReadThreadCount);
        WriteThreadPool_->Configure(config->WriteThreadCount);
    }

private:
    const IThreadPoolPtr ReadThreadPool_;
    const IThreadPoolPtr WriteThreadPool_;
    const IPrioritizedInvokerPtr ReadInvoker_;
    const IPrioritizedInvokerPtr WriteInvoker_;
};

class TPoolWeightProvider
    : public IPoolWeightProvider
{
public:
    TPoolWeightProvider(double defaultPoolWeight, double userInteractivePoolWeight)
        : DefaultPoolWeight_(defaultPoolWeight)
        , UserInteractivePoolWeight_(userInteractivePoolWeight)
    { }

    double GetWeight(const TString& poolName) override {
        if (poolName == "Default") {
            return DefaultPoolWeight_;
        } else if (poolName == "UserInteractive") {
            return UserInteractivePoolWeight_;
        } else {
            return 1.0;
        }
    }

private:
    const double DefaultPoolWeight_;
    const double UserInteractivePoolWeight_;
};

class TFairShareThreadPool
{
public:
    TFairShareThreadPool(
        TThreadPoolIOEngineConfigPtr config,
        const TString& locationId,
        NLogging::TLogger logger)
        : ReadThreadPool_(CreateNewTwoLevelFairShareThreadPool(
            config->ReadThreadCount,
            Format("FSH:%v", locationId),
            {
                New<TPoolWeightProvider>(config->DefaultPoolWeight, config->UserInteractivePoolWeight)
            }))
        , WriteThreadPool_(CreateThreadPool(config->WriteThreadCount, Format("IOW:%v", locationId)))
        , WriteInvoker_(CreatePrioritizedInvoker(WriteThreadPool_->GetInvoker(), NProfiling::TTagSet({{"invoker", "io_fair_share_thread_pool_writer"}, {"location_id", locationId}})))
        , Logger(logger)
        , DefaultPool_{"Default", config->DefaultPoolWeight}
        , UserInteractivePool_{"UserInteractive", config->UserInteractivePoolWeight}
    { }

    IInvokerPtr GetReadInvoker(EWorkloadCategory category, TIOEngineBase::TSessionId client)
    {
        const auto& pool = GetPoolByCategory(category);
        return ReadThreadPool_->GetInvoker(pool.Name, ToString(client));
    }

    IInvokerPtr GetWriteInvoker(EWorkloadCategory category, TIOEngineBase::TSessionId)
    {
        return CreateFixedPriorityInvoker(WriteInvoker_, GetBasicPriority(category));
    }

    void Reconfigure(const TThreadPoolIOEngineConfigPtr& config)
    {
        ReadThreadPool_->Configure(config->ReadThreadCount);
        WriteThreadPool_->Configure(config->WriteThreadCount);
    }

private:
    struct TPoolDescriptor
    {
        TString Name;
        double Weight = 1.0;
    };

    const TPoolDescriptor& GetPoolByCategory(EWorkloadCategory category)
    {
        if (category == EWorkloadCategory::UserInteractive) {
            return UserInteractivePool_;
        }
        return DefaultPool_;
    }

private:
    const ITwoLevelFairShareThreadPoolPtr ReadThreadPool_;
    const IThreadPoolPtr WriteThreadPool_;
    const IPrioritizedInvokerPtr WriteInvoker_;
    const NLogging::TLogger Logger;

    const TPoolDescriptor DefaultPool_;
    const TPoolDescriptor UserInteractivePool_;
};

class TTwoLevelPoolWeightProvider
    : public IPoolWeightProvider
{
public:
    TTwoLevelPoolWeightProvider(THashMap<TString, double> poolWeights)
        : PoolWeights_(std::move(poolWeights))
    { }

    double GetWeight(const TString& poolName) override {
        if (auto it = PoolWeights_.find(poolName); it != PoolWeights_.end()) {
            return it->second;
        } else {
            return 1.0;
        }
    }

    void Configure(THashMap<TString, double> poolWeights) {
        PoolWeights_ = std::move(poolWeights);
    }

private:
    THashMap<TString, double> PoolWeights_;
};

struct TBucketCacheEntry final
{
    NProfiling::TCpuInstant LastAccessTime;
    TString PoolName;
    TString BucketName;
    IInvokerPtr InvokerPtr;
};

bool operator < (const TIntrusivePtr<TBucketCacheEntry>& lhs, const TIntrusivePtr<TBucketCacheEntry>& rhs) {
    return std::tie(lhs->LastAccessTime, lhs->PoolName, lhs->BucketName) < std::tie(rhs->LastAccessTime, rhs->PoolName, rhs->BucketName);
}

// Caches pointers to buckets in order to prolong their live(and progress) between calls.
class TBucketCache final
{
public:
    TBucketCache(
        TDuration bucketTtl,
        NLogging::TLogger logger)
        : EntryTtl(bucketTtl)
        , Logger(logger)
    { }

    IInvokerPtr Get(
        const ITwoLevelFairShareThreadPoolPtr threadPool,
        const TString& poolName,
        const TString& bucketTag,
        const double bucketWeight)
    {
        auto guard = Guard(Lock);
        auto now = NProfiling::GetCpuInstant();
        FlushByTtl(now);

        auto entry = KeyToEntry.find(std::make_pair(poolName, bucketTag));
        if (entry != KeyToEntry.end()) {
            EntriesSet.erase(entry->second);
            entry->second->LastAccessTime = now;
            EntriesSet.insert(entry->second);
            return entry->second->InvokerPtr;
        } else {
            YT_LOG_DEBUG("TBucketCache miss for %v %v", poolName, bucketTag);
            auto newEntry = New<TBucketCacheEntry>();
            newEntry->LastAccessTime = now;
            newEntry->PoolName = poolName;
            newEntry->BucketName = bucketTag;
            newEntry->InvokerPtr = threadPool->GetInvoker(poolName, bucketTag, bucketWeight);
            KeyToEntry[std::make_pair(poolName, bucketTag)] = newEntry;
            EntriesSet.insert(newEntry);
            return newEntry->InvokerPtr;
        }
    }

private:
    // Should be called with Lock acquired.
    void FlushByTtl(NProfiling::TCpuInstant now) {
        while (!EntriesSet.empty() && (*EntriesSet.begin())->LastAccessTime + NProfiling::DurationToCpuDuration(EntryTtl) < now) {
            auto entry = *EntriesSet.begin();
            YT_LOG_DEBUG("TBucketCache erase by ttl for %v %v", entry->PoolName, entry->BucketName);
            EntriesSet.erase(EntriesSet.begin());
            KeyToEntry.erase(std::make_pair(entry->PoolName, entry->BucketName));
        }
    }

private:
    const TDuration EntryTtl;
    const NLogging::TLogger Logger;
    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, Lock);
    THashMap<std::pair<TString, TString>, TIntrusivePtr<TBucketCacheEntry>> KeyToEntry;
    TSet<TIntrusivePtr<TBucketCacheEntry>> EntriesSet;
};

class TTwoLevelFairShareThreadPool
{
public:
    TTwoLevelFairShareThreadPool(
        TThreadPoolIOEngineConfigPtr config,
        const TString& locationId,
        NLogging::TLogger logger)
        : PoolWeights_(New<TTwoLevelPoolWeightProvider>(config->PoolWeights))
        , ReadThreadPool_(CreateNewTwoLevelFairShareThreadPool(
            config->ReadThreadCount,
            Format("LRead:%v", locationId),
            {
                PoolWeights_,
                true // TODO change to false verbose logging
            }))
        , WriteThreadPool_(CreateNewTwoLevelFairShareThreadPool(
            config->WriteThreadCount,
            Format("LWrite:%v", locationId),
            {
                PoolWeights_,
                true // TODO change to false verbose logging
            }))
        , Logger(logger)
        , BucketCache_(New<TBucketCache>(config->BucketTtl, logger))
    { }

    IInvokerPtr GetReadInvoker(TWorkloadDescriptor workloadDescriptor, TIOEngineBase::TSessionId sessionId)
    {
        const auto& poolName = ToString(workloadDescriptor.Category);
        const auto& bucketTag = GetBucketTag(workloadDescriptor, sessionId);
        const auto bucketWeight = GetBucketWeight(workloadDescriptor);
        if (!sessionId) {
            YT_LOG_DEBUG("No sessionId received for GetReadInvoker");
        }
        return BucketCache_->Get(ReadThreadPool_, poolName, bucketTag, bucketWeight);
    }

    IInvokerPtr GetWriteInvoker(TWorkloadDescriptor workloadDescriptor, TIOEngineBase::TSessionId sessionId)
    {
        const auto& poolName = ToString(workloadDescriptor.Category);
        const auto& bucketTag = GetBucketTag(workloadDescriptor, sessionId);
        const auto bucketWeight = GetBucketWeight(workloadDescriptor);
        if (!sessionId) {
            YT_LOG_DEBUG("No sessionId received for GetWriteInvoker");
        }
        return BucketCache_->Get(WriteThreadPool_, poolName, bucketTag, bucketWeight);
    }

    void Reconfigure(const TThreadPoolIOEngineConfigPtr& config)
    {
        PoolWeights_->Configure(config->PoolWeights);
        ReadThreadPool_->Configure(config->ReadThreadCount);
        WriteThreadPool_->Configure(config->WriteThreadCount);
    }

private:
    TString GetBucketTag(TWorkloadDescriptor workloadDescriptor, TIOEngineBase::TSessionId sessionId) {
        if (workloadDescriptor.DiskFairShareBucketTag) {
            YT_LOG_DEBUG("Getting bucket tag from workload descriptor: %v", *workloadDescriptor.DiskFairShareBucketTag);
            return *workloadDescriptor.DiskFairShareBucketTag;
        } else if (sessionId) {
            YT_LOG_DEBUG("Getting bucket tag from sessionId: %v", ToString(sessionId));
            return ToString(sessionId);
        }
        return ToString(sessionId);
    }

    double GetBucketWeight(TWorkloadDescriptor workloadDescriptor) {
        if (workloadDescriptor.DiskFairShareBucketWeight && *workloadDescriptor.DiskFairShareBucketWeight > 0) {
            YT_LOG_DEBUG("Getting bucket weight from workload descriptor: %v", ToString(*workloadDescriptor.DiskFairShareBucketWeight));
            return *workloadDescriptor.DiskFairShareBucketWeight;
        } else {
            YT_LOG_DEBUG("Getting bucket weight from default value: 1.0");
            return 1.0;
        }
    }

private:
    TIntrusivePtr<TTwoLevelPoolWeightProvider> PoolWeights_;
    const ITwoLevelFairShareThreadPoolPtr ReadThreadPool_;
    const ITwoLevelFairShareThreadPoolPtr WriteThreadPool_;
    const NLogging::TLogger Logger;
    TIntrusivePtr<TBucketCache> BucketCache_;
};

template <typename TThreadPool, typename TRequestSlicer>
class TThreadPoolIOEngine
    : public TIOEngineBase
{
public:
    using TConfig = TThreadPoolIOEngineConfig;
    using TConfigPtr = TIntrusivePtr<TConfig>;

    TThreadPoolIOEngine(
        TConfigPtr config,
        TString locationId,
        TProfiler profiler,
        NLogging::TLogger logger)
        : TIOEngineBase(
            config,
            std::move(locationId),
            std::move(profiler),
            std::move(logger))
        , StaticConfig_(std::move(config))
        , Config_(StaticConfig_)
        , ThreadPool_(StaticConfig_, LocationId_, Logger)
        , RequestSlicer_(StaticConfig_->DesiredRequestSize, StaticConfig_->MinRequestSize)
    { }

    TFuture<TReadResponse> Read(
        std::vector<TReadRequest> requests,
        EWorkloadCategory category,
        TRefCountedTypeCookie tagCookie,
        TSessionId sessionId,
        bool useDedicatedAllocations) override
    {
        std::vector<TFuture<void>> futures;
        futures.reserve(requests.size());

        auto invoker = ThreadPool_.GetReadInvoker(category, sessionId);

        i64 paddedBytes = 0;
        for (const auto& request : requests) {
            paddedBytes += GetPaddedSize(request.Offset, request.Size, DefaultPageSize);
        }
        auto buffers = AllocateReadBuffers(requests, tagCookie, useDedicatedAllocations);

        for (int index = 0; index < std::ssize(requests); ++index) {
            for (auto& slice : RequestSlicer_.Slice(std::move(requests[index]), buffers[index])) {
                futures.push_back(
                    BIND(&TThreadPoolIOEngine::DoRead,
                        MakeStrong(this),
                        std::move(slice.Request),
                        std::move(slice.OutputBuffer),
                        TWallTimer(),
                        category,
                        sessionId)
                    .AsyncVia(invoker)
                    .Run());
            }
        }

        TReadResponse response{
            .PaddedBytes = paddedBytes,
            .IORequests = std::ssize(futures),
        };
        response.OutputBuffers.assign(buffers.begin(), buffers.end());

        return AllSucceeded(std::move(futures))
            .Apply(BIND([response = std::move(response)] () mutable {
                return std::move(response);
            }));
    }

    TFuture<void> Write(
        TWriteRequest request,
        EWorkloadCategory category,
        TSessionId sessionId) override
    {
        YT_ASSERT(request.Handle);
        std::vector<TFuture<void>> futures;
        for (auto& slice : RequestSlicer_.Slice(std::move(request))) {
            futures.push_back(
                BIND(&TThreadPoolIOEngine::DoWrite, MakeStrong(this), std::move(slice), TWallTimer())
                .AsyncVia(ThreadPool_.GetWriteInvoker(category, sessionId))
                .Run());
        }
        return AllSucceeded(std::move(futures));
    }

    TFuture<void> FlushFile(
        TFlushFileRequest request,
        EWorkloadCategory category) override
    {
        return BIND(&TThreadPoolIOEngine::DoFlushFile, MakeStrong(this), std::move(request))
            .AsyncVia(ThreadPool_.GetWriteInvoker(category, {}))
            .Run();
    }

    virtual TFuture<void> FlushFileRange(
        TFlushFileRangeRequest request,
        EWorkloadCategory category,
        TSessionId sessionId) override
    {
        std::vector<TFuture<void>> futures;
        for (auto& slice : RequestSlicer_.Slice(std::move(request))) {
            futures.push_back(
                BIND(&TThreadPoolIOEngine::DoFlushFileRange, MakeStrong(this), std::move(slice))
                .AsyncVia(ThreadPool_.GetWriteInvoker(category, sessionId))
                .Run());
        }
        return AllSucceeded(std::move(futures));
    }

private:
    const TConfigPtr StaticConfig_;
    TAtomicIntrusivePtr<TConfig> Config_;

    TThreadPool ThreadPool_;
    TRequestSlicer RequestSlicer_;


    std::vector<TSharedMutableRef> AllocateReadBuffers(
        const std::vector<TReadRequest>& requests,
        TRefCountedTypeCookie tagCookie,
        bool useDedicatedAllocations)
    {
        bool shouldBeAligned = std::any_of(
            requests.begin(),
            requests.end(),
            [] (const TReadRequest& request) {
                return request.Handle->IsOpenForDirectIO();
            });

        auto allocate = [&] (size_t size) {
            TSharedMutableRefAllocateOptions options{
                .InitializeStorage = false
            };
            return shouldBeAligned
                ? TSharedMutableRef::AllocatePageAligned(size, options, tagCookie)
                : TSharedMutableRef::Allocate(size, options, tagCookie);
        };

        std::vector<TSharedMutableRef> results;
        results.reserve(requests.size());

        if (useDedicatedAllocations) {
            for (const auto& request : requests) {
                results.push_back(allocate(request.Size));
            }
            return results;
        }

        // Collocate blocks in single buffer.
        i64 totalSize = 0;
        for (const auto& request : requests) {
            totalSize += shouldBeAligned
                ? AlignUp<i64>(request.Size, DefaultPageSize)
                : request.Size;
        }

        auto buffer = allocate(totalSize);
        i64 offset = 0;
        for (const auto& request : requests) {
            results.push_back(buffer.Slice(offset, offset + request.Size));
            offset += shouldBeAligned
                ? AlignUp<i64>(request.Size, DefaultPageSize)
                : request.Size;
        }
        return results;
    }

    void DoRead(
        const TReadRequest& request,
        TMutableRef buffer,
        TWallTimer timer,
        EWorkloadCategory category,
        TSessionId sessionId)
    {
        YT_VERIFY(std::ssize(buffer) == request.Size);

        const auto readWaitTime = timer.GetElapsedTime();
        AddReadWaitTimeSample(readWaitTime);
        Sensors_->UpdateKernelStatistics();

        auto toReadRemaining = static_cast<i64>(buffer.Size());
        auto fileOffset = request.Offset;
        i64 bufferOffset = 0;

        YT_LOG_DEBUG_IF(category == EWorkloadCategory::UserInteractive,
            "Started reading from disk (Handle: %v, RequestSize: %v, ReadSessionId: %v, ReadWaitTime: %v)",
            static_cast<FHANDLE>(*request.Handle),
            request.Size,
            sessionId,
            readWaitTime);

        NFS::WrapIOErrors([&] {
            auto config = Config_.Acquire();

            while (toReadRemaining > 0) {
                auto toRead = static_cast<ui32>(Min(toReadRemaining, config->MaxBytesPerRead));

                i64 reallyRead;
                {
                    TRequestStatsGuard statsGuard(Sensors_->ReadSensors);
                    NTracing::TNullTraceContextGuard nullTraceContextGuard;
                    reallyRead = HandleEintr(::pread, *request.Handle, buffer.Begin() + bufferOffset, toRead, fileOffset);

                    YT_LOG_DEBUG_IF(category == EWorkloadCategory::UserInteractive,
                        "Finished reading from disk (Handle: %v, ReadBytes: %v, ReadSessionId: %v, ReadTime: %v)",
                        static_cast<FHANDLE>(*request.Handle),
                        reallyRead,
                        sessionId,
                        statsGuard.GetElapsedTime());
                }

                if (reallyRead < 0) {
                    // TODO(aozeritsky): ythrow is placed here consciously.
                    // WrapIOErrors rethrows some kind of arcadia-style exception.
                    // So in order to keep the old behaviour we should use ythrow or
                    // rewrite WrapIOErrors.
                    ythrow TFileError();
                }

                if (reallyRead == 0) {
                    break;
                }

                Sensors_->RegisterReadBytes(reallyRead);
                if (StaticConfig_->SimulatedMaxBytesPerRead) {
                    reallyRead = Min(reallyRead, *StaticConfig_->SimulatedMaxBytesPerRead);
                }

                fileOffset += reallyRead;
                bufferOffset += reallyRead;
                toReadRemaining -= reallyRead;
            }
        });

        if (toReadRemaining > 0) {
            THROW_ERROR_EXCEPTION(NFS::EErrorCode::IOError, "Unexpected end-of-file in read request")
                << TErrorAttribute("to_read_remaining", toReadRemaining)
                << TErrorAttribute("max_bytes_per_read", StaticConfig_->MaxBytesPerRead)
                << TErrorAttribute("request_size", request.Size)
                << TErrorAttribute("request_offset", request.Offset)
                << TErrorAttribute("file_size", request.Handle->GetLength())
                << TErrorAttribute("handle", static_cast<FHANDLE>(*request.Handle));
        }
    }

    void DoWrite(
        const TWriteRequest& request,
        TWallTimer timer)
    {
        auto writtenBytes = DoWriteImpl(request, timer);

        auto config = Config_.Acquire();
        if (config->FlushAfterWrite && request.Flush && writtenBytes) {
            DoFlushFileRange(TFlushFileRangeRequest{
                .Handle = request.Handle,
                .Offset = request.Offset,
                .Size = writtenBytes
            });
        } else if (config->AsyncFlushAfterWrite && writtenBytes) {
            DoFlushFileRange(TFlushFileRangeRequest{
                .Handle = request.Handle,
                .Offset = request.Offset,
                .Size = writtenBytes,
                .Async = true
            });
        }
    }

    i64 DoWriteImpl(
        const TWriteRequest& request,
        TWallTimer timer)
    {
        AddWriteWaitTimeSample(timer.GetElapsedTime());
        Sensors_->UpdateKernelStatistics();

        auto fileOffset = request.Offset;

        NFS::WrapIOErrors([&] {
            NTracing::TNullTraceContextGuard nullTraceContextGuard;

            auto toWriteRemaining = static_cast<i64>(GetByteSize(request.Buffers));

            int bufferIndex = 0;
            i64 bufferOffset = 0; // within current buffer

            auto config = Config_.Acquire();
            while (toWriteRemaining > 0) {
                auto isPwritevSupported = [&] {
#ifdef _linux_
                    return true;
#else
                    return false;
#endif
                };

                auto pwritev = [&] {
#ifdef _linux_
                    std::array<iovec, MaxIovCountPerRequest> iov;
                    int iovCount = 0;
                    i64 toWrite = 0;
                    while (bufferIndex + iovCount < std::ssize(request.Buffers) &&
                           iovCount < std::ssize(iov) &&
                           toWrite < config->MaxBytesPerWrite)
                    {
                        const auto& buffer = request.Buffers[bufferIndex + iovCount];
                        auto& iovPart = iov[iovCount];
                        iovPart = {
                            .iov_base = const_cast<char*>(buffer.Begin()),
                            .iov_len = buffer.Size()
                        };
                        if (iovCount == 0) {
                            iovPart.iov_base = static_cast<char*>(iovPart.iov_base) + bufferOffset;
                            iovPart.iov_len -= bufferOffset;
                        }
                        if (toWrite + static_cast<i64>(iovPart.iov_len) > config->MaxBytesPerWrite) {
                            iovPart.iov_len = config->MaxBytesPerWrite - toWrite;
                        }
                        toWrite += iovPart.iov_len;
                        ++iovCount;
                    }

                    i64 reallyWritten;
                    {
                        TRequestStatsGuard statsGuard(Sensors_->WriteSensors);
                        NTracing::TNullTraceContextGuard nullTraceContextGuard;
                        reallyWritten = HandleEintr(::pwritev, *request.Handle, iov.data(), iovCount, fileOffset);
                    }

                    if (reallyWritten < 0) {
                        ythrow TFileError();
                    }

                    Sensors_->RegisterWrittenBytes(reallyWritten);
                    if (StaticConfig_->SimulatedMaxBytesPerWrite) {
                        reallyWritten = Min(reallyWritten, *StaticConfig_->SimulatedMaxBytesPerWrite);
                    }

                    while (reallyWritten > 0) {
                        const auto& buffer = request.Buffers[bufferIndex];
                        i64 toAdvance = Min(static_cast<i64>(buffer.Size()) - bufferOffset, reallyWritten);
                        fileOffset += toAdvance;
                        bufferOffset += toAdvance;
                        reallyWritten -= toAdvance;
                        toWriteRemaining -= toAdvance;
                        if (bufferOffset == std::ssize(buffer)) {
                            ++bufferIndex;
                            bufferOffset = 0;
                        }
                    }
#else
                    YT_ABORT();
#endif
                };

                auto pwrite = [&] {
                    const auto& buffer = request.Buffers[bufferIndex];
                    auto toWrite = static_cast<ui32>(Min(toWriteRemaining, config->MaxBytesPerWrite, static_cast<i64>(buffer.Size()) - bufferOffset));

                    i32 reallyWritten;
                    {
                        TRequestStatsGuard statsGuard(Sensors_->WriteSensors);
                        NTracing::TNullTraceContextGuard nullTraceContextGuard;
                        reallyWritten = HandleEintr(::pwrite, *request.Handle, const_cast<char*>(buffer.Begin()) + bufferOffset, toWrite, fileOffset);
                    }

                    if (reallyWritten < 0) {
                        ythrow TFileError();
                    }

                    Sensors_->RegisterWrittenBytes(reallyWritten);
                    fileOffset += reallyWritten;
                    bufferOffset += reallyWritten;
                    toWriteRemaining -= reallyWritten;
                    if (bufferOffset == std::ssize(buffer)) {
                        ++bufferIndex;
                        bufferOffset = 0;
                    }
                };

                if (config->EnablePwritev && isPwritevSupported()) {
                    pwritev();
                } else {
                    pwrite();
                }
            }
        });

        return fileOffset - request.Offset;
    }

    void DoFlushFile(const TFlushFileRequest& request)
    {
        Sensors_->UpdateKernelStatistics();
        if (!StaticConfig_->EnableSync) {
            return;
        }

        auto doFsync = [&] {
            TRequestStatsGuard statsGuard(Sensors_->SyncSensors);
            return HandleEintr(::fsync, *request.Handle);
        };

#ifdef _linux_
        auto doFdatasync = [&] {
            TRequestStatsGuard statsGuard(Sensors_->DataSyncSensors);
            return HandleEintr(::fdatasync, *request.Handle);
        };
#else
        auto doFdatasync = doFsync;
#endif

        NFS::WrapIOErrors([&] {
            NTracing::TNullTraceContextGuard nullTraceContextGuard;
            int result;
            switch (request.Mode) {
                case EFlushFileMode::All:
                    result = doFsync();
                    break;
                case EFlushFileMode::Data:
                    result = doFdatasync();
                    break;
                default:
                    YT_ABORT();
            }
            if (result != 0) {
                ythrow TFileError();
            }
        });
    }

    void DoFlushFileRange(const TFlushFileRangeRequest& request)
    {
        Sensors_->UpdateKernelStatistics();
        if (!StaticConfig_->EnableSync) {
            return;
        }

#ifdef _linux_
        NFS::WrapIOErrors([&] {
            NTracing::TNullTraceContextGuard nullTraceContextGuard;
            int result = 0;
            {
                TRequestStatsGuard statsGuard(Sensors_->DataSyncSensors);
                const auto flags = request.Async
                    ? SYNC_FILE_RANGE_WRITE
                    : SYNC_FILE_RANGE_WAIT_BEFORE | SYNC_FILE_RANGE_WRITE | SYNC_FILE_RANGE_WAIT_AFTER;
                result = HandleEintr(::sync_file_range, *request.Handle, request.Offset, request.Size, flags);
            };
            if (result != 0) {
                ythrow TFileError();
            }
        });
#else

    Y_UNUSED(request);

#endif

    }

    void DoReconfigure(const NYTree::INodePtr& node) override
    {
        auto config = UpdateYsonStruct(StaticConfig_, node);

        ThreadPool_.Reconfigure(config);
        Config_.Store(config);
    }
};

////////////////////////////////////////////////////////////////////////////////

IIOEnginePtr CreateIOEngine(
    EIOEngineType engineType,
    NYTree::INodePtr ioConfig,
    TString locationId,
    TProfiler profiler,
    NLogging::TLogger logger)
{
    using TClassicThreadPoolIOEngine = TThreadPoolIOEngine<TFixedPriorityExecutor, TDummyRequestSlicer>;
    using TFairShareThreadPoolIOEngine = TThreadPoolIOEngine<TFairShareThreadPool, TIORequestSlicer>;
    using TTwoLevelFairShareThreadPoolIOEngine = TThreadPoolIOEngine<TTwoLevelFairShareThreadPool, TIORequestSlicer>;

    switch (engineType) {
        case EIOEngineType::ThreadPool:
            return CreateIOEngine<TClassicThreadPoolIOEngine>(
                std::move(ioConfig),
                std::move(locationId),
                std::move(profiler),
                std::move(logger));
#ifdef _linux_
        case EIOEngineType::Uring:
        case EIOEngineType::FairShareUring:
            return CreateIOEngineUring(
                engineType,
                std::move(ioConfig),
                std::move(locationId),
                std::move(profiler),
                std::move(logger));
#endif
        case EIOEngineType::FairShareThreadPool:
            return CreateIOEngine<TFairShareThreadPoolIOEngine>(
                std::move(ioConfig),
                std::move(locationId),
                std::move(profiler),
                std::move(logger));
        case NYT::NIO::EIOEngineType::TwoLevelFairShareThreadPool:
            return CreateIOEngine<TTwoLevelFairShareThreadPoolIOEngine>(
                std::move(ioConfig),
                std::move(locationId),
                std::move(profiler),
                std::move(logger)
            );
        default:
            THROW_ERROR_EXCEPTION("Unknown IO engine %Qlv",
                engineType);
    }
}

std::vector<EIOEngineType> GetSupportedIOEngineTypes()
{
    std::vector<EIOEngineType> result;
    result.push_back(EIOEngineType::ThreadPool);
    if (IsUringIOEngineSupported()) {
        result.push_back(EIOEngineType::Uring);
        result.push_back(EIOEngineType::FairShareUring);
    }
    result.push_back(EIOEngineType::FairShareThreadPool);
    return result;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NIO
