using System.Threading.Channels;

namespace Yatk;

/// <summary>
/// FIFO キュー上でジョブを固定数まで並列実行するスケジューラです。
/// </summary>
public sealed class YatkScheduler : IAsyncDisposable
{
    private readonly object syncRoot = new();
    private readonly Channel<JobEntry> queue;
    private readonly Dictionary<YatkJobId, JobEntry> jobs = new();
    private readonly Task[] workers;
    private bool isDisposed;

    /// <summary>
    /// 指定した最大並列度でスケジューラを初期化します。
    /// </summary>
    /// <param name="maxConcurrency">同時に実行するジョブの最大数です。</param>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="maxConcurrency"/> が 1 未満の場合に発生します。</exception>
    public YatkScheduler(int maxConcurrency = 1)
    {
        if (maxConcurrency < 1)
        {
            throw new ArgumentOutOfRangeException(nameof(maxConcurrency));
        }

        queue = Channel.CreateUnbounded<JobEntry>(new UnboundedChannelOptions
        {
            SingleWriter = false,
            SingleReader = false,
        });
        workers = new Task[maxConcurrency];

        for (var index = 0; index < workers.Length; index++)
        {
            workers[index] = ProcessQueueAsync();
        }
    }

    /// <summary>
    /// ラムダ式のジョブをキューへ投入します。
    /// </summary>
    /// <param name="action">実行する非同期処理です。</param>
    /// <param name="name">ジョブを識別する任意の名前です。</param>
    /// <returns>投入したジョブの ID です。</returns>
    /// <exception cref="ArgumentNullException"><paramref name="action"/> が <see langword="null"/> の場合に発生します。</exception>
    /// <exception cref="ObjectDisposedException">スケジューラが破棄済みの場合に発生します。</exception>
    public YatkJobId Do(Func<CancellationToken, Task> action, string? name = null)
    {
        ArgumentNullException.ThrowIfNull(action);

        var job = new DelegateJob(action, name);
        return Enqueue(job);
    }

    /// <summary>
    /// リッチジョブをキューへ投入します。
    /// </summary>
    /// <param name="job">実行するジョブです。</param>
    /// <returns>投入したジョブの ID です。</returns>
    /// <exception cref="ArgumentNullException"><paramref name="job"/> が <see langword="null"/> の場合に発生します。</exception>
    /// <exception cref="InvalidOperationException">ジョブがすでに投入済みの場合に発生します。</exception>
    /// <exception cref="ObjectDisposedException">スケジューラが破棄済みの場合に発生します。</exception>
    public YatkJobId Enqueue(YatkJobBase job)
    {
        ArgumentNullException.ThrowIfNull(job);

        lock (syncRoot)
        {
            ThrowIfDisposed();

            if (!job.TryMarkQueued(DateTimeOffset.UtcNow))
            {
                throw new InvalidOperationException("同じジョブを複数回投入することはできません。");
            }

            var entry = new JobEntry(job);
            jobs.Add(job.JobId, entry);
            if (!queue.Writer.TryWrite(entry))
            {
                throw new InvalidOperationException("ジョブをキューへ投入できませんでした。");
            }

            return job.JobId;
        }
    }

    /// <summary>
    /// 指定した ID のジョブ状態を取得します。
    /// </summary>
    /// <param name="jobId">取得するジョブの ID です。</param>
    /// <returns>ジョブが存在する場合はスナップショット。それ以外の場合は <see langword="null"/> です。</returns>
    public YatkJobSnapshot? GetJob(YatkJobId jobId)
    {
        lock (syncRoot)
        {
            return jobs.TryGetValue(jobId, out var entry) ? entry.Job.CreateSnapshot() : null;
        }
    }

    /// <summary>
    /// 登録済みのすべてのジョブ状態を取得します。
    /// </summary>
    /// <returns>ジョブのスナップショット一覧です。</returns>
    public IReadOnlyList<YatkJobSnapshot> GetJobs()
    {
        lock (syncRoot)
        {
            return jobs.Values.Select(entry => entry.Job.CreateSnapshot()).ToArray();
        }
    }

    /// <summary>
    /// 指定したジョブのキャンセルを要求します。
    /// </summary>
    /// <param name="jobId">キャンセルするジョブの ID です。</param>
    /// <returns>キャンセル要求を受理した場合は <see langword="true"/>。対象ジョブが存在しない、またはキャンセルできない状態の場合は <see langword="false"/> です。</returns>
    public bool Cancel(YatkJobId jobId)
    {
        CancellationTokenSource? cancellationTokenSource = null;

        lock (syncRoot)
        {
            if (!jobs.TryGetValue(jobId, out var entry))
            {
                return false;
            }

            if (entry.Job.TryMarkQueuedCanceled(DateTimeOffset.UtcNow))
            {
                return true;
            }

            if (!entry.Job.TryMarkCancelRequested())
            {
                return false;
            }

            cancellationTokenSource = entry.CancellationTokenSource;
        }

        cancellationTokenSource.Cancel();
        return true;
    }

    /// <summary>
    /// 指定したジョブが終了状態になるまで待機します。
    /// </summary>
    /// <param name="jobId">待機するジョブの ID です。</param>
    /// <param name="cancellationToken">待機のみを中断するためのトークンです。</param>
    /// <returns>待機処理を表すタスクです。</returns>
    /// <exception cref="KeyNotFoundException">指定したジョブが存在しない場合に発生します。</exception>
    /// <exception cref="OperationCanceledException"><paramref name="cancellationToken"/> により待機が中断された場合に発生します。</exception>
    public async Task WaitForCompletionAsync(YatkJobId jobId, CancellationToken cancellationToken = default)
    {
        JobEntry entry;

        lock (syncRoot)
        {
            if (!jobs.TryGetValue(jobId, out entry))
            {
                throw new KeyNotFoundException("指定したジョブは登録されていません。");
            }
        }

        await entry.Job.WaitForCompletionAsync(cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// 新規投入を終了し、キューにあるジョブと実行中ジョブの終了を待機します。
    /// </summary>
    /// <returns>破棄処理を表す値です。</returns>
    public async ValueTask DisposeAsync()
    {
        lock (syncRoot)
        {
            if (isDisposed)
            {
                return;
            }

            isDisposed = true;
            queue.Writer.TryComplete();
        }

        await Task.WhenAll(workers).ConfigureAwait(false);

        lock (syncRoot)
        {
            foreach (var entry in jobs.Values)
            {
                entry.CancellationTokenSource.Dispose();
            }
        }
    }

    private async Task ProcessQueueAsync()
    {
        await foreach (var entry in queue.Reader.ReadAllAsync().ConfigureAwait(false))
        {
            if (!TryStart(entry))
            {
                continue;
            }

            try
            {
                await entry.Job.ExecuteInternalAsync(entry.CancellationTokenSource.Token).ConfigureAwait(false);
                entry.Job.MarkSucceeded(DateTimeOffset.UtcNow);
            }
            catch (OperationCanceledException exception) when (IsCancellationForJob(entry, exception))
            {
                entry.Job.TryMarkCanceledAfterCancellation(DateTimeOffset.UtcNow);
            }
            catch (Exception exception)
            {
                entry.Job.MarkFailed(DateTimeOffset.UtcNow, exception);
            }
        }
    }

    private bool TryStart(JobEntry entry)
    {
        lock (syncRoot)
        {
            return entry.Job.TryMarkRunning(DateTimeOffset.UtcNow);
        }
    }

    private static bool IsCancellationForJob(JobEntry entry, OperationCanceledException exception)
    {
        return entry.CancellationTokenSource.IsCancellationRequested
            && exception.CancellationToken == entry.CancellationTokenSource.Token;
    }

    private void ThrowIfDisposed()
    {
        if (isDisposed)
        {
            throw new ObjectDisposedException(nameof(YatkScheduler));
        }
    }

    private sealed class DelegateJob : YatkJobBase
    {
        private readonly Func<CancellationToken, Task> action;

        public DelegateJob(Func<CancellationToken, Task> action, string? name)
            : base(name)
        {
            this.action = action;
        }

        protected override Task ExecuteAsync(CancellationToken cancellationToken)
        {
            return action(cancellationToken);
        }
    }

    private sealed class JobEntry
    {
        public JobEntry(YatkJobBase job)
        {
            Job = job;
            CancellationTokenSource = new CancellationTokenSource();
        }

        public YatkJobBase Job { get; }

        public CancellationTokenSource CancellationTokenSource { get; }
    }
}
