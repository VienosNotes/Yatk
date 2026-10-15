namespace Yatk;

/// <summary>
/// FIFO キュー上でジョブを指定した最大数まで並列実行するスケジューラです。
/// </summary>
public sealed class YatkScheduler : IAsyncDisposable
{
    /// <summary>
    /// タイマーに一度に設定する待ち時間の上限です。
    /// </summary>
    /// <remarks>
    /// <see cref="TimeProvider.System"/> のタイマーは 0xFFFFFFFE ミリ秒（約 49.7 日）を超える待ち時間を受け付けないため、
    /// これを超える遅延は上限で区切って待機し、発火時に残りの時間で設定し直す。
    /// </remarks>
    private static readonly TimeSpan MaxTimerDueTime = TimeSpan.FromMilliseconds(uint.MaxValue - 1);

    /// <summary>
    /// タイマーに設定する待ち時間の下限です。
    /// </summary>
    /// <remarks>
    /// <see cref="TimeProvider"/> の実装によっては、期限切れの待ち時間を設定するとロックを保持したままコールバックが同期的に呼ばれる。
    /// 待ち時間を常に正にして、タイマーの設定中にコールバックが再入しないようにする。
    /// </remarks>
    private static readonly TimeSpan MinTimerDueTime = TimeSpan.FromMilliseconds(1);

    private readonly object syncRoot = new();
    private readonly LinkedList<JobEntry> queuedJobs = new();
    private readonly LinkedList<JobEntry> highPriorityQueuedJobs = new();
    private readonly SortedSet<JobEntry> delayedJobs = new(DelayedJobComparer.Instance);
    private readonly Dictionary<YatkJobId, JobEntry> jobs = new();
    private readonly Queue<YatkJobId> completedJobIds = new();
    private readonly Queue<YatkJobSnapshot> pendingJobChanges = new();
    private readonly TimeProvider timeProvider;
    private int maxConcurrency;
    private readonly int maxRetainedCompletedJobs;
    private long nextSequence;
    private ITimer? delayTimer;
    private int runningCount;
    private bool isDispatchingJobChanges;
    private Task? stopTask;
    private TaskCompletionSource? stopCompletion;

    /// <summary>
    /// ジョブの状態、進捗、状態メッセージが変更されたときに発生します。
    /// </summary>
    public event EventHandler<YatkJobChangedEventArgs>? JobChanged;

    /// <summary>
    /// 指定した最大並列度でスケジューラを初期化します。
    /// </summary>
    /// <param name="maxConcurrency">同時に実行するジョブの最大数です。</param>
    /// <param name="timeProvider">時刻の取得と遅延実行の待機に使う <see cref="TimeProvider"/> です。<see langword="null"/> の場合は <see cref="TimeProvider.System"/> を使います。</param>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="maxConcurrency"/> が 1 未満の場合に発生します。</exception>
    public YatkScheduler(int maxConcurrency = 1, TimeProvider? timeProvider = null)
        : this(new YatkSchedulerOptions
        {
            MaxConcurrency = maxConcurrency,
            TimeProvider = timeProvider ?? TimeProvider.System,
        })
    {
    }

    /// <summary>
    /// 指定した設定でスケジューラを初期化します。
    /// </summary>
    /// <param name="options">初期設定です。</param>
    /// <exception cref="ArgumentNullException"><paramref name="options"/> または <see cref="YatkSchedulerOptions.TimeProvider"/> が <see langword="null"/> の場合に発生します。</exception>
    /// <exception cref="ArgumentOutOfRangeException"><see cref="YatkSchedulerOptions.MaxConcurrency"/> が 1 未満の場合に発生します。</exception>
    public YatkScheduler(YatkSchedulerOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);
        ArgumentNullException.ThrowIfNull(options.TimeProvider, nameof(options));
        ValidateMaxConcurrency(options.MaxConcurrency);
        ValidateMaxRetainedCompletedJobs(options.MaxRetainedCompletedJobs);

        maxConcurrency = options.MaxConcurrency;
        maxRetainedCompletedJobs = options.MaxRetainedCompletedJobs;
        timeProvider = options.TimeProvider;
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
    /// 保持する完了済みジョブの最大数を取得します。
    /// </summary>
    public int MaxRetainedCompletedJobs => maxRetainedCompletedJobs;

    /// <summary>
    /// ラムダ式のジョブをキューへ投入します。
    /// </summary>
    /// <param name="action">実行する非同期処理です。</param>
    /// <param name="name">ジョブを識別する任意の名前です。</param>
    /// <param name="priority">ジョブの実行優先度です。</param>
    /// <param name="delay">実行可能になるまでの待ち時間です。0 以下の場合は直ちに待機キューへ加えます。</param>
    /// <returns>投入したジョブの ID です。</returns>
    /// <exception cref="ArgumentNullException"><paramref name="action"/> が <see langword="null"/> の場合に発生します。</exception>
    /// <exception cref="ArgumentOutOfRangeException">現在時刻に <paramref name="delay"/> を加えた時刻が表現できる範囲を超える場合に発生します。</exception>
    /// <exception cref="ObjectDisposedException">スケジューラが停止済みの場合に発生します。</exception>
    public YatkJobId Do(
        Func<CancellationToken, Task> action,
        string? name = null,
        YatkJobPriority priority = YatkJobPriority.Normal,
        TimeSpan delay = default)
    {
        ArgumentNullException.ThrowIfNull(action);

        return Do((_, cancellationToken) => action(cancellationToken), name, priority, delay);
    }

    /// <summary>
    /// コンテキストを受け取るラムダ式のジョブをキューへ投入します。
    /// </summary>
    /// <param name="action">実行する非同期処理です。</param>
    /// <param name="name">ジョブを識別する任意の名前です。</param>
    /// <param name="priority">ジョブの実行優先度です。</param>
    /// <param name="delay">実行可能になるまでの待ち時間です。0 以下の場合は直ちに待機キューへ加えます。</param>
    /// <returns>投入したジョブの ID です。</returns>
    /// <exception cref="ArgumentNullException"><paramref name="action"/> が <see langword="null"/> の場合に発生します。</exception>
    /// <exception cref="ArgumentOutOfRangeException">現在時刻に <paramref name="delay"/> を加えた時刻が表現できる範囲を超える場合に発生します。</exception>
    /// <exception cref="ObjectDisposedException">スケジューラが停止済みの場合に発生します。</exception>
    public YatkJobId Do(
        Func<YatkJobContext, CancellationToken, Task> action,
        string? name = null,
        YatkJobPriority priority = YatkJobPriority.Normal,
        TimeSpan delay = default)
    {
        ArgumentNullException.ThrowIfNull(action);

        return Enqueue(new DelegateJob(action, name), priority, delay);
    }

    /// <summary>
    /// リッチジョブをキューへ投入します。
    /// </summary>
    /// <param name="job">実行するジョブです。</param>
    /// <param name="priority">ジョブの実行優先度です。</param>
    /// <param name="delay">
    /// 実行可能になるまでの待ち時間です。
    /// 正の値を指定すると、ジョブは <see cref="YatkJobState.Delayed"/> で登録され、待ち時間の経過後に待機キューへ加わります。
    /// 0 以下の場合は直ちに待機キューへ加えます。
    /// </param>
    /// <returns>投入したジョブの ID です。</returns>
    /// <exception cref="ArgumentNullException"><paramref name="job"/> が <see langword="null"/> の場合に発生します。</exception>
    /// <exception cref="ArgumentOutOfRangeException">現在時刻に <paramref name="delay"/> を加えた時刻が表現できる範囲を超える場合に発生します。</exception>
    /// <exception cref="InvalidOperationException">ジョブがすでに投入済みの場合に発生します。</exception>
    /// <exception cref="ObjectDisposedException">スケジューラが停止済みの場合に発生します。</exception>
    public YatkJobId Enqueue(
        YatkJobBase job,
        YatkJobPriority priority = YatkJobPriority.Normal,
        TimeSpan delay = default)
    {
        ArgumentNullException.ThrowIfNull(job);
        ValidatePriority(priority);

        lock (syncRoot)
        {
            ThrowIfStopping();

            var now = timeProvider.GetUtcNow();
            if (delay > TimeSpan.Zero)
            {
                if (delay > DateTimeOffset.MaxValue - now)
                {
                    throw new ArgumentOutOfRangeException(nameof(delay));
                }

                if (!job.TryMarkDelayed(now))
                {
                    throw new InvalidOperationException("同じジョブを複数回投入することはできません。");
                }

                var entry = new JobEntry(job, priority, now + delay, nextSequence++);
                jobs.Add(job.JobId, entry);
                EnqueueJobChanged(CreateSnapshot(entry));
                delayedJobs.Add(entry);

                // 先頭が変わった場合だけ、タイマーの期限を早める必要がある。
                if (delayedJobs.Min == entry)
                {
                    RescheduleDelayTimer(now);
                }
            }
            else
            {
                if (!job.TryMarkQueued(now))
                {
                    throw new InvalidOperationException("同じジョブを複数回投入することはできません。");
                }

                var entry = new JobEntry(job, priority, null, nextSequence++);
                jobs.Add(job.JobId, entry);
                EnqueueJobChanged(CreateSnapshot(entry));
                AdmitQueuedJob(entry);
            }
        }

        DispatchJobChanges();
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
            return jobs.TryGetValue(jobId, out var entry) ? CreateSnapshot(entry) : null;
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
            return jobs.Values.Select(CreateSnapshot).ToArray();
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

            if (entry.Job.TryMarkCanceledBeforeRunning(timeProvider.GetUtcNow()))
            {
                RemoveQueuedJob(entry);
                if (delayedJobs.Remove(entry))
                {
                    RescheduleDelayTimer(timeProvider.GetUtcNow());
                }

                snapshot = CreateSnapshot(entry);
                RecordCompletion(entry);
            }
            else if (entry.Job.TryMarkCancelRequested(timeProvider.GetUtcNow()))
            {
                cancellationTokenSource = entry.CancellationTokenSource;
                snapshot = CreateSnapshot(entry);
            }
            else
            {
                return false;
            }

            EnqueueJobChanged(snapshot);
        }

        if (cancellationTokenSource is not null)
        {
            RequestCancellation(cancellationTokenSource);
        }

        DispatchJobChanges();
        return true;
    }

    /// <summary>
    /// 指定したジョブが終了状態になるまで待機します。
    /// </summary>
    /// <param name="jobId">待機するジョブの ID です。</param>
    /// <param name="cancellationToken">待機のみを中断するためのトークンです。</param>
    /// <returns>ジョブを見つけて完了まで待機できた場合は <see langword="true"/>。指定したジョブが未登録または保持期限切れの場合は <see langword="false"/> です。</returns>
    /// <exception cref="OperationCanceledException"><paramref name="cancellationToken"/> により待機が中断された場合に発生します。</exception>
    public async Task<bool> WaitForCompletionAsync(YatkJobId jobId, CancellationToken cancellationToken = default)
    {
        JobEntry? entry;

        lock (syncRoot)
        {
            if (!jobs.TryGetValue(jobId, out entry) || entry is null)
            {
                return false;
            }
        }

        await entry.Job.WaitForCompletionAsync(cancellationToken).ConfigureAwait(false);
        return true;
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

        DispatchJobChanges();
    }

    /// <summary>
    /// 新規投入を停止し、指定した方法で登録済みジョブを終了させます。
    /// </summary>
    /// <param name="mode">停止方法です。</param>
    /// <param name="cancellationToken">停止処理の待機のみを中断するためのトークンです。</param>
    /// <returns>停止処理を表すタスクです。</returns>
    public Task StopAsync(YatkShutdownMode mode, CancellationToken cancellationToken = default)
    {
        if (!Enum.IsDefined(mode))
        {
            throw new ArgumentOutOfRangeException(nameof(mode));
        }

        List<CancellationTokenSource>? cancellationTokenSources = null;
        ITimer? timerToDispose = null;
        Task currentStopTask;

        lock (syncRoot)
        {
            if (stopTask is null)
            {
                stopCompletion = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

                // どちらの停止方法でも、時刻が来る前のジョブは実行せずに取り消す。
                foreach (var delayedEntry in delayedJobs)
                {
                    if (delayedEntry.Job.TryMarkCanceledBeforeRunning(timeProvider.GetUtcNow()))
                    {
                        EnqueueJobChanged(CreateSnapshot(delayedEntry));
                        RecordCompletion(delayedEntry);
                    }
                }

                delayedJobs.Clear();
                timerToDispose = delayTimer;
                delayTimer = null;

                if (mode == YatkShutdownMode.Cancel)
                {
                    cancellationTokenSources = new List<CancellationTokenSource>();

                    while (TryTakeNextQueuedJob(out var queuedEntry))
                    {
                        if (queuedEntry.Job.TryMarkCanceledBeforeRunning(timeProvider.GetUtcNow()))
                        {
                            EnqueueJobChanged(CreateSnapshot(queuedEntry));
                            RecordCompletion(queuedEntry);
                        }
                    }

                    foreach (var entry in jobs.Values)
                    {
                        if (entry.Job.TryMarkCancelRequested(timeProvider.GetUtcNow()))
                        {
                            EnqueueJobChanged(CreateSnapshot(entry));
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

        timerToDispose?.Dispose();

        if (cancellationTokenSources is not null)
        {
            foreach (var cancellationTokenSource in cancellationTokenSources)
            {
                RequestCancellation(cancellationTokenSource);
            }
        }

        DispatchJobChanges();

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

    private async Task ProcessJobAsync(JobEntry entry)
    {
        try
        {
            await entry.Job.ExecuteInternalAsync(
                    entry.CancellationTokenSource.Token,
                    timeProvider,
                    () => OnJobReported(entry))
                .ConfigureAwait(false);
            MarkSucceeded(entry);
            DispatchJobChanges();
        }
        catch (OperationCanceledException exception) when (IsCancellationForJob(entry, exception))
        {
            TryMarkCanceledAfterCancellation(entry);
            DispatchJobChanges();
        }
        catch (Exception exception)
        {
            MarkFailed(entry, exception);
            DispatchJobChanges();
        }
        finally
        {
            OnJobExecutionFinished();
        }
    }

    /// <summary>
    /// <see cref="YatkJobState.Queued"/> になったジョブを、優先度に応じて開始または待機キューへ追加します。
    /// </summary>
    /// <remarks>
    /// 呼び出し元は <see cref="syncRoot"/> を保持している必要があります。
    /// </remarks>
    private void AdmitQueuedJob(JobEntry entry)
    {
        if (entry.Priority == YatkJobPriority.Immediate)
        {
            StartJob(entry);
        }
        else
        {
            var queue = entry.Priority == YatkJobPriority.High ? highPriorityQueuedJobs : queuedJobs;
            entry.QueueNode = queue.AddLast(entry);
            StartAvailableJobs();
        }
    }

    /// <summary>
    /// 遅延待ちのタイマーが発火したときに、時刻が来たジョブを待機キューへ加えます。
    /// </summary>
    /// <remarks>
    /// タイマーは「そろそろ確認する」合図としてだけ扱い、時刻が来たかどうかは <see cref="TimeProvider.GetUtcNow"/> で判定し直します。
    /// システム時刻の変更や、設定し直す前のタイマーの遅れた発火があっても、期限前のジョブは待機キューへ加えません。
    /// </remarks>
    private void OnDelayTimerFired()
    {
        // TimeProvider によっては、GetUtcNow などの呼び出し中にタイマーのコールバックを同期的に呼ぶ（FakeTimeProvider など）。
        // スケジューラのロック内で再入した場合に処理すると、呼び出し元の操作の途中で状態を変更してしまうため、ThreadPool で処理し直す。
        if (Monitor.IsEntered(syncRoot))
        {
            ThreadPool.UnsafeQueueUserWorkItem(_ => OnDelayTimerFired(), null);
            return;
        }

        lock (syncRoot)
        {
            // 停止後に遅れて発火した場合、遅延待ちのジョブはすでに取り消されている。
            if (stopTask is not null)
            {
                return;
            }

            var now = timeProvider.GetUtcNow();

            // NotBefore と投入順で整列しているため、先頭から取り出すことで順序を保つ。
            while (delayedJobs.Min is { } entry && entry.NotBefore <= now)
            {
                delayedJobs.Remove(entry);
                if (entry.Job.TryMarkReady(now))
                {
                    EnqueueJobChanged(CreateSnapshot(entry));
                    AdmitQueuedJob(entry);
                }
            }

            RescheduleDelayTimer(now);
        }

        DispatchJobChanges();
    }

    /// <summary>
    /// 遅延待ちの先頭ジョブの期限に合わせて、タイマーを設定し直します。
    /// </summary>
    /// <remarks>
    /// 呼び出し元は <see cref="syncRoot"/> を保持している必要があります。
    /// </remarks>
    private void RescheduleDelayTimer(DateTimeOffset now)
    {
        if (delayedJobs.Min is not { } next)
        {
            delayTimer?.Change(Timeout.InfiniteTimeSpan, Timeout.InfiniteTimeSpan);
            return;
        }

        var dueTime = next.NotBefore!.Value - now;
        if (dueTime < MinTimerDueTime)
        {
            dueTime = MinTimerDueTime;
        }
        else if (dueTime > MaxTimerDueTime)
        {
            dueTime = MaxTimerDueTime;
        }

        if (delayTimer is null)
        {
            // スケジューラが存続する間、最初の投入元の ExecutionContext をタイマーに保持させない。
            using (ExecutionContext.SuppressFlow())
            {
                delayTimer = timeProvider.CreateTimer(
                    _ => OnDelayTimerFired(),
                    null,
                    Timeout.InfiniteTimeSpan,
                    Timeout.InfiniteTimeSpan);
            }
        }

        delayTimer.Change(dueTime, Timeout.InfiniteTimeSpan);
    }

    private void StartAvailableJobs()
    {
        while (runningCount < maxConcurrency && TryTakeNextQueuedJob(out var entry))
        {
            StartJob(entry);
        }
    }

    private void StartJob(JobEntry entry)
    {
        if (!entry.Job.TryMarkRunning(timeProvider.GetUtcNow()))
        {
            return;
        }

        var runningSnapshot = CreateSnapshot(entry);
        EnqueueJobChanged(runningSnapshot);
        runningCount++;
        _ = Task.Run(() => ProcessJobAsync(entry));
    }

    /// <summary>
    /// ジョブから報告された進捗または状態メッセージを変更通知へ反映します。
    /// </summary>
    private void OnJobReported(JobEntry entry)
    {
        lock (syncRoot)
        {
            EnqueueJobChanged(CreateSnapshot(entry));
        }

        DispatchJobChanges();
    }

    private bool TryTakeNextQueuedJob(out JobEntry entry)
    {
        var queue = highPriorityQueuedJobs.First is not null ? highPriorityQueuedJobs : queuedJobs;
        if (queue.First is null)
        {
            entry = null!;
            return false;
        }

        entry = queue.First.Value;
        queue.RemoveFirst();
        entry.QueueNode = null;
        return true;
    }

    private void MarkSucceeded(JobEntry entry)
    {
        lock (syncRoot)
        {
            entry.Job.MarkSucceeded(timeProvider.GetUtcNow());
            var snapshot = CreateSnapshot(entry);
            RecordCompletion(entry);
            EnqueueJobChanged(snapshot);
        }
    }

    private void TryMarkCanceledAfterCancellation(JobEntry entry)
    {
        lock (syncRoot)
        {
            if (!entry.Job.TryMarkCanceledAfterCancellation(timeProvider.GetUtcNow()))
            {
                return;
            }

            var snapshot = CreateSnapshot(entry);
            RecordCompletion(entry);
            EnqueueJobChanged(snapshot);
        }
    }

    private void MarkFailed(JobEntry entry, Exception exception)
    {
        lock (syncRoot)
        {
            entry.Job.MarkFailed(timeProvider.GetUtcNow(), exception);
            var snapshot = CreateSnapshot(entry);
            RecordCompletion(entry);
            EnqueueJobChanged(snapshot);
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

        DispatchJobChanges();
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
        if (stopCompletion is not null
            && queuedJobs.Count == 0
            && highPriorityQueuedJobs.Count == 0
            && runningCount == 0)
        {
            stopCompletion.TrySetResult();
        }
    }

    private void RecordCompletion(JobEntry entry)
    {
        // 完了済みジョブの保持期間中に、クロージャが捕捉したオブジェクトを残さない。
        if (entry.Job is DelegateJob delegateJob)
        {
            delegateJob.ReleaseAction();
        }

        completedJobIds.Enqueue(entry.Job.JobId);

        while (completedJobIds.Count > maxRetainedCompletedJobs)
        {
            var expiredJobId = completedJobIds.Dequeue();
            if (jobs.Remove(expiredJobId, out var expiredEntry))
            {
                expiredEntry.CancellationTokenSource.Dispose();
            }
        }
    }

    private void RemoveQueuedJob(JobEntry entry)
    {
        if (entry.QueueNode is not null)
        {
            entry.QueueNode.List?.Remove(entry.QueueNode);
            entry.QueueNode = null;
        }
    }

    private static bool IsCancellationForJob(JobEntry entry, OperationCanceledException exception)
    {
        return entry.CancellationTokenSource.IsCancellationRequested
            && exception.CancellationToken == entry.CancellationTokenSource.Token;
    }

    private static void RequestCancellation(CancellationTokenSource cancellationTokenSource)
    {
        try
        {
            cancellationTokenSource.Cancel();
        }
        catch (ObjectDisposedException)
        {
            // 完了処理との競合で既に破棄されている場合は、キャンセル通知が不要になっている。
        }
    }

    /// <summary>
    /// 状態遷移と同じ同期範囲で、変更通知を配送待ちキューへ追加します。
    /// </summary>
    /// <remarks>
    /// 状態を変更したスレッドが直ちにイベントハンドラを呼ぶと、別スレッドで先に発生した通知との順序が逆転し得ます。
    /// このメソッドで通知をキューへ記録し、<see cref="DispatchJobChanges"/> が FIFO 順に配送することで、状態遷移の順序を保ちます。
    /// 呼び出し元は <see cref="syncRoot"/> を保持している必要があります。
    /// </remarks>
    private void EnqueueJobChanged(YatkJobSnapshot snapshot)
    {
        pendingJobChanges.Enqueue(snapshot);
    }

    /// <summary>
    /// 配送待ちの状態変更通知を、イベントハンドラへ FIFO 順に配送します。
    /// </summary>
    /// <remarks>
    /// 一度に一つの呼び出しだけが配送役になります。配送中に別スレッドで新しい通知が追加された場合、
    /// その呼び出しは何もせず、配送役が同じキューから続けて通知します。
    /// イベントハンドラは <see cref="syncRoot"/> の外で実行するため、ハンドラからスケジューラ API を呼び出してもデッドロックしません。
    /// </remarks>
    private void DispatchJobChanges()
    {
        lock (syncRoot)
        {
            if (isDispatchingJobChanges)
            {
                return;
            }

            isDispatchingJobChanges = true;
        }

        while (true)
        {
            YatkJobSnapshot? snapshot;

            lock (syncRoot)
            {
                if (pendingJobChanges.Count == 0)
                {
                    isDispatchingJobChanges = false;
                    return;
                }

                snapshot = pendingJobChanges.Dequeue();
            }

            InvokeJobChanged(snapshot);
        }
    }

    /// <summary>
    /// 一つのスナップショットに対するイベントハンドラを、例外を隔離して実行します。
    /// </summary>
    private void InvokeJobChanged(YatkJobSnapshot snapshot)
    {
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

    private static void ValidateMaxRetainedCompletedJobs(int maxRetainedCompletedJobs)
    {
        if (maxRetainedCompletedJobs < 0)
        {
            throw new ArgumentOutOfRangeException(nameof(maxRetainedCompletedJobs));
        }
    }

    private static void ValidatePriority(YatkJobPriority priority)
    {
        if (!Enum.IsDefined(priority))
        {
            throw new ArgumentOutOfRangeException(nameof(priority));
        }
    }

    private static YatkJobSnapshot CreateSnapshot(JobEntry entry)
    {
        return entry.Job.CreateSnapshot() with
        {
            Priority = entry.Priority,
            NotBefore = entry.NotBefore,
        };
    }

    private sealed class DelegateJob : YatkJobBase
    {
        private Func<YatkJobContext, CancellationToken, Task>? action;

        public DelegateJob(Func<YatkJobContext, CancellationToken, Task> action, string? name)
            : base(name)
        {
            this.action = action;
        }

        /// <summary>
        /// 実行されずに終了状態になったジョブのデリゲートへの参照を解放します。
        /// </summary>
        /// <remarks>
        /// 終了状態になった後は実行されないため、実行中のスレッドと競合しない。
        /// </remarks>
        public void ReleaseAction()
        {
            action = null;
        }

        protected override Task ExecuteAsync(YatkJobContext context, CancellationToken cancellationToken)
        {
            // ジョブは一度だけ実行されるため、開始時点でフィールドからの参照を手放す。
            // これにより、完了の通知より前にデリゲートが解放される。
            // 実行前に終了状態になったジョブは実行されないため、ここでは解放されていない。
            var currentAction = action!;
            action = null;
            return currentAction(context, cancellationToken);
        }
    }

    private sealed class JobEntry
    {
        public JobEntry(YatkJobBase job, YatkJobPriority priority, DateTimeOffset? notBefore, long sequence)
        {
            Job = job;
            Priority = priority;
            NotBefore = notBefore;
            Sequence = sequence;
            CancellationTokenSource = new CancellationTokenSource();
        }

        public YatkJobBase Job { get; }

        public YatkJobPriority Priority { get; }

        /// <summary>
        /// 遅延を指定して投入された場合の、実行可能になる時刻です。
        /// </summary>
        public DateTimeOffset? NotBefore { get; }

        /// <summary>
        /// 投入順を表す連番です。同じ <see cref="NotBefore"/> のジョブを投入順に並べるために使います。
        /// </summary>
        public long Sequence { get; }

        public CancellationTokenSource CancellationTokenSource { get; }

        public LinkedListNode<JobEntry>? QueueNode { get; set; }
    }

    /// <summary>
    /// 遅延待ちのジョブを、実行可能になる時刻、投入順の順に並べる比較子です。
    /// </summary>
    private sealed class DelayedJobComparer : IComparer<JobEntry>
    {
        public static readonly DelayedJobComparer Instance = new();

        public int Compare(JobEntry? x, JobEntry? y)
        {
            if (ReferenceEquals(x, y))
            {
                return 0;
            }

            if (x is null)
            {
                return -1;
            }

            if (y is null)
            {
                return 1;
            }

            var result = x.NotBefore.GetValueOrDefault().CompareTo(y.NotBefore.GetValueOrDefault());
            return result != 0 ? result : x.Sequence.CompareTo(y.Sequence);
        }
    }
}
