using System.Threading.Channels;

namespace Yatk;

/// <summary>
/// FIFO キュー上でジョブを固定数まで並列実行するスケジューラです。
/// </summary>
public sealed class YatkScheduler : IAsyncDisposable
{
    private readonly object syncRoot = new();
    private readonly Channel<YatkJobBase> queue;
    private readonly Dictionary<YatkJobId, YatkJobBase> jobs = new();
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

        queue = Channel.CreateUnbounded<YatkJobBase>(new UnboundedChannelOptions
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

            jobs.Add(job.JobId, job);
            if (!queue.Writer.TryWrite(job))
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
            return jobs.TryGetValue(jobId, out var job) ? job.CreateSnapshot() : null;
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
            return jobs.Values.Select(job => job.CreateSnapshot()).ToArray();
        }
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
    }

    private async Task ProcessQueueAsync()
    {
        await foreach (var job in queue.Reader.ReadAllAsync().ConfigureAwait(false))
        {
            job.MarkRunning(DateTimeOffset.UtcNow);

            try
            {
                await job.ExecuteInternalAsync(CancellationToken.None).ConfigureAwait(false);
                job.MarkSucceeded(DateTimeOffset.UtcNow);
            }
            catch (Exception exception)
            {
                job.MarkFailed(DateTimeOffset.UtcNow, exception);
            }
        }
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
}
