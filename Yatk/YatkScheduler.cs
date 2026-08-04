namespace Yatk;

/// <summary>
/// FIFO キュー上でジョブを指定した最大数まで並列実行するスケジューラです。
/// </summary>
public sealed class YatkScheduler : IAsyncDisposable
{
    private readonly object syncRoot = new();
    private readonly Queue<JobEntry> queuedJobs = new();
    private readonly Dictionary<YatkJobId, JobEntry> jobs = new();
    private int maxConcurrency;
    private int runningCount;
    private Task? stopTask;
    private TaskCompletionSource? stopCompletion;

    /// <summary>
    /// ジョブの状態が変更されたときに発生します。
    /// </summary>
    public event EventHandler<YatkJobChangedEventArgs>? JobChanged;

    /// <summary>
    /// 指定した最大並列度でスケジューラを初期化します。
    /// </summary>
    /// <param name="maxConcurrency">同時に実行するジョブの最大数です。</param>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="maxConcurrency"/> が 1 未満の場合に発生します。</exception>
    public YatkScheduler(int maxConcurrency = 1)
    {
        ValidateMaxConcurrency(maxConcurrency);
        this.maxConcurrency = maxConcurrency;
    }

    /// <summary>
    /// 指定した設定でスケジューラを初期化します。
    /// </summary>
    /// <param name="options">初期設定です。</param>
    /// <exception cref="ArgumentNullException"><paramref name="options"/> が <see langword="null"/> の場合に発生します。</exception>
    /// <exception cref="ArgumentOutOfRangeException"><see cref="YatkSchedulerOptions.MaxConcurrency"/> が 1 未満の場合に発生します。</exception>
    public YatkScheduler(YatkSchedulerOptions options)
        : this((options ?? throw new ArgumentNullException(nameof(options))).MaxConcurrency)
    {
    }

    /// <summary>
    /// 現在の最大並列度を取得します。
    /// </summary>
    public int MaxConcurrency
    {
        get
        {
            lock (syncRoot)
            {
                return maxConcurrency;
            }
        }
    }

    /// <summary>
    /// ラムダ式のジョブをキューへ投入します。
    /// </summary>
    /// <param name="action">実行する非同期処理です。</param>
    /// <param name="name">ジョブを識別する任意の名前です。</param>
    /// <returns>投入したジョブの ID です。</returns>
    /// <exception cref="ArgumentNullException"><paramref name="action"/> が <see langword="null"/> の場合に発生します。</exception>
    /// <exception cref="ObjectDisposedException">スケジューラが停止済みの場合に発生します。</exception>
    public YatkJobId Do(Func<CancellationToken, Task> action, string? name = null)
    {
        ArgumentNullException.ThrowIfNull(action);

        return Do((_, cancellationToken) => action(cancellationToken), name);
    }

    /// <summary>
    /// コンテキストを受け取るラムダ式のジョブをキューへ投入します。
    /// </summary>
    /// <param name="action">実行する非同期処理です。</param>
    /// <param name="name">ジョブを識別する任意の名前です。</param>
    /// <returns>投入したジョブの ID です。</returns>
    /// <exception cref="ArgumentNullException"><paramref name="action"/> が <see langword="null"/> の場合に発生します。</exception>
    /// <exception cref="ObjectDisposedException">スケジューラが停止済みの場合に発生します。</exception>
    public YatkJobId Do(Func<YatkJobContext, CancellationToken, Task> action, string? name = null)
    {
        ArgumentNullException.ThrowIfNull(action);

        return Enqueue(new DelegateJob(action, name));
    }

    /// <summary>
    /// リッチジョブをキューへ投入します。
    /// </summary>
    /// <param name="job">実行するジョブです。</param>
    /// <returns>投入したジョブの ID です。</returns>
    /// <exception cref="ArgumentNullException"><paramref name="job"/> が <see langword="null"/> の場合に発生します。</exception>
    /// <exception cref="InvalidOperationException">ジョブがすでに投入済みの場合に発生します。</exception>
    /// <exception cref="ObjectDisposedException">スケジューラが停止済みの場合に発生します。</exception>
    public YatkJobId Enqueue(YatkJobBase job)
    {
        ArgumentNullException.ThrowIfNull(job);

        YatkJobSnapshot snapshot;

        lock (syncRoot)
        {
            ThrowIfStopping();

            if (!job.TryMarkQueued(DateTimeOffset.UtcNow))
            {
                throw new InvalidOperationException("同じジョブを複数回投入することはできません。");
            }

            var entry = new JobEntry(job);
            jobs.Add(job.JobId, entry);
            queuedJobs.Enqueue(entry);
            snapshot = job.CreateSnapshot();
            StartAvailableJobs();
        }

        RaiseJobChanged(snapshot);
        return job.JobId;
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
        YatkJobSnapshot? snapshot = null;

        lock (syncRoot)
        {
            if (!jobs.TryGetValue(jobId, out var entry))
            {
                return false;
            }

            if (entry.Job.TryMarkQueuedCanceled(DateTimeOffset.UtcNow))
            {
                snapshot = entry.Job.CreateSnapshot();
            }
            else if (entry.Job.TryMarkCancelRequested())
            {
                cancellationTokenSource = entry.CancellationTokenSource;
                snapshot = entry.Job.CreateSnapshot();
            }
            else
            {
                return false;
            }
        }

        RaiseJobChanged(snapshot);
        cancellationTokenSource?.Cancel();
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
        JobEntry? entry;

        lock (syncRoot)
        {
            if (!jobs.TryGetValue(jobId, out entry) || entry is null)
            {
                throw new KeyNotFoundException("指定したジョブは登録されていません。");
            }
        }

        await entry.Job.WaitForCompletionAsync(cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// 最大並列度を変更します。
    /// </summary>
    /// <param name="maxConcurrency">変更後の最大並列度です。</param>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="maxConcurrency"/> が 1 未満の場合に発生します。</exception>
    public void SetMaxConcurrency(int maxConcurrency)
    {
        ValidateMaxConcurrency(maxConcurrency);

        lock (syncRoot)
        {
            this.maxConcurrency = maxConcurrency;
            StartAvailableJobs();
        }
    }

    /// <summary>
    /// 新規投入を停止し、指定した方法で登録済みジョブを終了させます。
    /// </summary>
    /// <param name="mode">停止方法です。</param>
    /// <param name="cancellationToken">停止処理の待機のみを中断するためのトークンです。</param>
    /// <returns>停止処理を表すタスクです。</returns>
    public Task StopAsync(YatkShutdownMode mode, CancellationToken cancellationToken = default)
    {
        List<YatkJobSnapshot>? snapshots = null;
        List<CancellationTokenSource>? cancellationTokenSources = null;
        Task currentStopTask;

        lock (syncRoot)
        {
            if (stopTask is null)
            {
                stopCompletion = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

                if (mode == YatkShutdownMode.Cancel)
                {
                    snapshots = new List<YatkJobSnapshot>();
                    cancellationTokenSources = new List<CancellationTokenSource>();

                    while (queuedJobs.Count > 0)
                    {
                        var entry = queuedJobs.Dequeue();
                        if (entry.Job.TryMarkQueuedCanceled(DateTimeOffset.UtcNow))
                        {
                            snapshots.Add(entry.Job.CreateSnapshot());
                        }
                    }

                    foreach (var entry in jobs.Values)
                    {
                        if (entry.Job.TryMarkCancelRequested())
                        {
                            snapshots.Add(entry.Job.CreateSnapshot());
                            cancellationTokenSources.Add(entry.CancellationTokenSource);
                        }
                    }
                }
                else
                {
                    StartAvailableJobs();
                }

                TryCompleteStop();
                stopTask = StopCoreAsync();
            }

            currentStopTask = stopTask;
        }

        if (snapshots is not null)
        {
            foreach (var snapshot in snapshots)
            {
                RaiseJobChanged(snapshot);
            }
        }

        if (cancellationTokenSources is not null)
        {
            foreach (var cancellationTokenSource in cancellationTokenSources)
            {
                cancellationTokenSource.Cancel();
            }
        }

        return currentStopTask.WaitAsync(cancellationToken);
    }

    /// <summary>
    /// キャンセル方式でスケジューラを停止します。
    /// </summary>
    /// <returns>破棄処理を表す値です。</returns>
    public async ValueTask DisposeAsync()
    {
        await StopAsync(YatkShutdownMode.Cancel).ConfigureAwait(false);
    }

    private async Task ProcessJobAsync(JobEntry entry, YatkJobSnapshot runningSnapshot)
    {
        RaiseJobChanged(runningSnapshot);

        try
        {
            await entry.Job.ExecuteInternalAsync(entry.CancellationTokenSource.Token).ConfigureAwait(false);
            RaiseJobChanged(MarkSucceeded(entry));
        }
        catch (OperationCanceledException exception) when (IsCancellationForJob(entry, exception))
        {
            var canceledSnapshot = TryMarkCanceledAfterCancellation(entry);
            if (canceledSnapshot is not null)
            {
                RaiseJobChanged(canceledSnapshot);
            }
        }
        catch (Exception exception)
        {
            RaiseJobChanged(MarkFailed(entry, exception));
        }
        finally
        {
            OnJobExecutionFinished();
        }
    }

    private void StartAvailableJobs()
    {
        while (runningCount < maxConcurrency && queuedJobs.Count > 0)
        {
            var entry = queuedJobs.Dequeue();
            if (!entry.Job.TryMarkRunning(DateTimeOffset.UtcNow))
            {
                continue;
            }

            var runningSnapshot = entry.Job.CreateSnapshot();
            runningCount++;
            _ = Task.Run(() => ProcessJobAsync(entry, runningSnapshot));
        }
    }

    private YatkJobSnapshot MarkSucceeded(JobEntry entry)
    {
        lock (syncRoot)
        {
            entry.Job.MarkSucceeded(DateTimeOffset.UtcNow);
            return entry.Job.CreateSnapshot();
        }
    }

    private YatkJobSnapshot? TryMarkCanceledAfterCancellation(JobEntry entry)
    {
        lock (syncRoot)
        {
            if (!entry.Job.TryMarkCanceledAfterCancellation(DateTimeOffset.UtcNow))
            {
                return null;
            }

            return entry.Job.CreateSnapshot();
        }
    }

    private YatkJobSnapshot MarkFailed(JobEntry entry, Exception exception)
    {
        lock (syncRoot)
        {
            entry.Job.MarkFailed(DateTimeOffset.UtcNow, exception);
            return entry.Job.CreateSnapshot();
        }
    }

    private void OnJobExecutionFinished()
    {
        lock (syncRoot)
        {
            runningCount--;
            StartAvailableJobs();
            TryCompleteStop();
        }
    }

    private async Task StopCoreAsync()
    {
        Task completion;

        lock (syncRoot)
        {
            completion = stopCompletion!.Task;
        }

        await completion.ConfigureAwait(false);

        lock (syncRoot)
        {
            foreach (var entry in jobs.Values)
            {
                entry.CancellationTokenSource.Dispose();
            }
        }
    }

    private void TryCompleteStop()
    {
        if (stopCompletion is not null && queuedJobs.Count == 0 && runningCount == 0)
        {
            stopCompletion.TrySetResult();
        }
    }

    private static bool IsCancellationForJob(JobEntry entry, OperationCanceledException exception)
    {
        return entry.CancellationTokenSource.IsCancellationRequested
            && exception.CancellationToken == entry.CancellationTokenSource.Token;
    }

    private void RaiseJobChanged(YatkJobSnapshot? snapshot)
    {
        if (snapshot is null)
        {
            return;
        }

        var handlers = JobChanged;
        if (handlers is null)
        {
            return;
        }

        var eventArgs = new YatkJobChangedEventArgs(snapshot);
        foreach (EventHandler<YatkJobChangedEventArgs> handler in handlers.GetInvocationList())
        {
            try
            {
                handler(this, eventArgs);
            }
            catch (Exception)
            {
                // イベントハンドラの例外はスケジューラの制御を継続するために無視する。
            }
        }
    }

    private void ThrowIfStopping()
    {
        if (stopTask is not null)
        {
            throw new ObjectDisposedException(nameof(YatkScheduler));
        }
    }

    private static void ValidateMaxConcurrency(int maxConcurrency)
    {
        if (maxConcurrency < 1)
        {
            throw new ArgumentOutOfRangeException(nameof(maxConcurrency));
        }
    }

    private sealed class DelegateJob : YatkJobBase
    {
        private readonly Func<YatkJobContext, CancellationToken, Task> action;

        public DelegateJob(Func<YatkJobContext, CancellationToken, Task> action, string? name)
            : base(name)
        {
            this.action = action;
        }

        protected override Task ExecuteAsync(YatkJobContext context, CancellationToken cancellationToken)
        {
            return action(context, cancellationToken);
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
