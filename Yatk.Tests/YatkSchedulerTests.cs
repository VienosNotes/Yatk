using System.Collections.Concurrent;

namespace Yatk.Tests;

public sealed class YatkSchedulerTests
{
    // ラムダジョブが実行されることを確認する。
    [Fact]
    public async Task Do_ExecutesLambdaJob()
    {
        var completed = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var scheduler = new YatkScheduler();

        scheduler.Do(_ =>
        {
            completed.SetResult();
            return Task.CompletedTask;
        });

        await completed.Task.WaitAsync(TimeSpan.FromSeconds(5));
    }

    // YatkJobBase 派生ジョブが実行されることを確認する。
    [Fact]
    public async Task Enqueue_ExecutesRichJob()
    {
        var job = new TestJob();
        await using var scheduler = new YatkScheduler();

        scheduler.Enqueue(job);

        await job.Completed.Task.WaitAsync(TimeSpan.FromSeconds(5));
    }

    // 最大並列度が 1 のときにジョブが FIFO 順で開始されることを確認する。
    [Fact]
    public async Task Jobs_AreStartedInFifoOrder_WhenConcurrencyIsOne()
    {
        var started = new ConcurrentQueue<int>();
        var firstStarted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var firstCanFinish = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var secondStarted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var scheduler = new YatkScheduler(maxConcurrency: 1);

        scheduler.Do(async _ =>
        {
            started.Enqueue(1);
            firstStarted.SetResult();
            await firstCanFinish.Task;
        });
        scheduler.Do(_ =>
        {
            started.Enqueue(2);
            secondStarted.SetResult();
            return Task.CompletedTask;
        });

        await firstStarted.Task.WaitAsync(TimeSpan.FromSeconds(5));
        Assert.Equal(new[] { 1 }, started);
        firstCanFinish.SetResult();
        await secondStarted.Task.WaitAsync(TimeSpan.FromSeconds(5));
        Assert.Equal(new[] { 1, 2 }, started);
    }

    // 実行中ジョブ数が最大並列度を超えないことを確認する。
    [Fact]
    public async Task Jobs_DoNotExceedConfiguredMaxConcurrency()
    {
        const int maxConcurrency = 2;
        var started = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var running = 0;
        var maximumObserved = 0;
        await using var scheduler = new YatkScheduler(maxConcurrency);

        for (var index = 0; index < 4; index++)
        {
            scheduler.Do(async _ =>
            {
                var current = Interlocked.Increment(ref running);
                UpdateMaximum(ref maximumObserved, current);
                if (current == maxConcurrency)
                {
                    started.TrySetResult();
                }

                await release.Task;
                Interlocked.Decrement(ref running);
            });
        }

        await started.Task.WaitAsync(TimeSpan.FromSeconds(5));
        Assert.Equal(maxConcurrency, maximumObserved);
        release.SetResult();
    }

    // 失敗したジョブの後も後続ジョブが実行されることを確認する。
    [Fact]
    public async Task FailedJob_DoesNotPreventLaterJobFromRunning()
    {
        var completed = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var scheduler = new YatkScheduler();

        scheduler.Do(_ => throw new InvalidOperationException("失敗"));
        scheduler.Do(_ =>
        {
            completed.SetResult();
            return Task.CompletedTask;
        });

        await completed.Task.WaitAsync(TimeSpan.FromSeconds(5));
    }

    // スナップショットにジョブの状態遷移と完了情報が記録されることを確認する。
    [Fact]
    public async Task Enqueue_RecordsLifecycleInSnapshot()
    {
        var started = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var scheduler = new YatkScheduler();

        var jobId = scheduler.Do(async _ =>
        {
            started.SetResult();
            await release.Task;
        }, "状態確認用");

        await started.Task.WaitAsync(TimeSpan.FromSeconds(5));
        var running = scheduler.GetJob(jobId);
        Assert.NotNull(running);
        Assert.Equal("状態確認用", running.Name);
        Assert.Equal(YatkJobState.Running, running.State);
        Assert.NotNull(running.QueuedAt);
        Assert.NotNull(running.StartedAt);
        Assert.Null(running.CompletedAt);

        release.SetResult();
        await scheduler.DisposeAsync();

        var completed = scheduler.GetJob(jobId);
        Assert.NotNull(completed);
        Assert.Equal(YatkJobState.Succeeded, completed.State);
        Assert.NotNull(completed.CompletedAt);
        Assert.Null(completed.Exception);
    }

    // 失敗したジョブの例外がスナップショットに記録されることを確認する。
    [Fact]
    public async Task FailedJob_RecordsExceptionInSnapshot()
    {
        await using var scheduler = new YatkScheduler();

        var jobId = scheduler.Do(_ => throw new InvalidOperationException("失敗"));
        await scheduler.DisposeAsync();

        var snapshot = scheduler.GetJob(jobId);
        Assert.NotNull(snapshot);
        var exception = Assert.IsType<InvalidOperationException>(snapshot.Exception);
        Assert.Equal("失敗", exception.Message);
        Assert.NotNull(snapshot.CompletedAt);
    }

    // ジョブ一覧がジョブ実体ではなくスナップショットを返すことを確認する。
    [Fact]
    public async Task GetJobs_ReturnsSnapshotsWithoutJobInstances()
    {
        await using var scheduler = new YatkScheduler();

        var jobId = scheduler.Do(_ => Task.CompletedTask);
        await scheduler.DisposeAsync();

        var snapshots = scheduler.GetJobs();

        var snapshot = Assert.Single(snapshots);
        Assert.Equal(jobId, snapshot.JobId);
    }

    // ジョブコンテキストから報告した進捗と状態メッセージがスナップショットに反映されることを確認する。
    [Fact]
    public async Task JobContext_ReportsProgressAndStatusMessage()
    {
        await using var scheduler = new YatkScheduler();
        YatkJobId contextJobId = default;

        var jobId = scheduler.Do((context, _) =>
        {
            contextJobId = context.JobId;
            context.ReportProgress(0.5);
            context.SetStatusMessage("処理中");
            return Task.CompletedTask;
        });

        await scheduler.WaitForCompletionAsync(jobId).WaitAsync(TimeSpan.FromSeconds(5));

        var snapshot = scheduler.GetJob(jobId);
        Assert.NotNull(snapshot);
        Assert.Equal(jobId, contextJobId);
        Assert.Equal(0.5, snapshot.Progress);
        Assert.Equal("処理中", snapshot.StatusMessage);
    }

    // 状態メッセージを null に設定するとスナップショットからクリアされることを確認する。
    [Fact]
    public async Task JobContext_ClearsStatusMessageWithNull()
    {
        await using var scheduler = new YatkScheduler();

        var jobId = scheduler.Do((context, _) =>
        {
            context.SetStatusMessage("処理中");
            context.SetStatusMessage(null);
            return Task.CompletedTask;
        });

        await scheduler.WaitForCompletionAsync(jobId).WaitAsync(TimeSpan.FromSeconds(5));

        Assert.Null(scheduler.GetJob(jobId)?.StatusMessage);
    }

    // 範囲外の進捗を報告するとジョブが失敗として記録されることを確認する。
    [Fact]
    public async Task JobContext_RejectsOutOfRangeProgress()
    {
        await using var scheduler = new YatkScheduler();

        var jobId = scheduler.Do((context, _) =>
        {
            context.ReportProgress(1.1);
            return Task.CompletedTask;
        });

        await scheduler.WaitForCompletionAsync(jobId).WaitAsync(TimeSpan.FromSeconds(5));

        var snapshot = scheduler.GetJob(jobId);
        Assert.NotNull(snapshot);
        Assert.Equal(YatkJobState.Failed, snapshot.State);
        Assert.IsType<ArgumentOutOfRangeException>(snapshot.Exception);
    }

    // 待機中にキャンセルされたジョブが実行されないことを確認する。
    [Fact]
    public async Task Cancel_DoesNotExecuteQueuedJob()
    {
        var firstStarted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var releaseFirst = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var secondExecuted = false;
        await using var scheduler = new YatkScheduler();

        scheduler.Do(async _ =>
        {
            firstStarted.SetResult();
            await releaseFirst.Task;
        });
        var jobId = scheduler.Do(_ =>
        {
            secondExecuted = true;
            return Task.CompletedTask;
        });

        await firstStarted.Task.WaitAsync(TimeSpan.FromSeconds(5));
        Assert.True(scheduler.Cancel(jobId));
        Assert.Equal(YatkJobState.Canceled, scheduler.GetJob(jobId)?.State);

        releaseFirst.SetResult();
        await scheduler.DisposeAsync();

        Assert.False(secondExecuted);
        Assert.NotNull(scheduler.GetJob(jobId)?.CompletedAt);
    }

    // 実行中ジョブへキャンセルトークンが渡り、キャンセル完了として記録されることを確認する。
    [Fact]
    public async Task Cancel_PropagatesTokenAndRecordsCanceled()
    {
        var started = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var cancellationObserved = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var scheduler = new YatkScheduler();

        var jobId = scheduler.Do(async cancellationToken =>
        {
            started.SetResult();
            using var registration = cancellationToken.Register(() => cancellationObserved.TrySetResult());
            await Task.Delay(Timeout.InfiniteTimeSpan, cancellationToken);
        });

        await started.Task.WaitAsync(TimeSpan.FromSeconds(5));
        Assert.True(scheduler.Cancel(jobId));
        await cancellationObserved.Task.WaitAsync(TimeSpan.FromSeconds(5));
        await scheduler.DisposeAsync();

        var snapshot = scheduler.GetJob(jobId);
        Assert.NotNull(snapshot);
        Assert.Equal(YatkJobState.Canceled, snapshot.State);
        Assert.NotNull(snapshot.CompletedAt);
    }

    // キャンセルを無視して正常終了したジョブが成功として記録されることを確認する。
    [Fact]
    public async Task Cancel_IgnoredByJobResultsInSucceeded()
    {
        var started = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var scheduler = new YatkScheduler();

        var jobId = scheduler.Do(async _ =>
        {
            started.SetResult();
            await release.Task;
        });

        await started.Task.WaitAsync(TimeSpan.FromSeconds(5));
        Assert.True(scheduler.Cancel(jobId));
        Assert.Equal(YatkJobState.CancelRequested, scheduler.GetJob(jobId)?.State);
        Assert.False(scheduler.Cancel(jobId));
        release.SetResult();
        await scheduler.DisposeAsync();

        Assert.Equal(YatkJobState.Succeeded, scheduler.GetJob(jobId)?.State);
    }

    // 完了済みまたは未登録のジョブをキャンセルできないことを確認する。
    [Fact]
    public async Task Cancel_ReturnsFalseForCompletedOrUnknownJob()
    {
        await using var scheduler = new YatkScheduler();

        var jobId = scheduler.Do(_ => Task.CompletedTask);
        await scheduler.DisposeAsync();

        Assert.False(scheduler.Cancel(jobId));
        Assert.False(scheduler.Cancel(new YatkJobId(Guid.NewGuid())));
    }

    // 正常終了したジョブを完了まで待機できることを確認する。
    [Fact]
    public async Task WaitForCompletionAsync_WaitsForSucceededJob()
    {
        var started = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var scheduler = new YatkScheduler();

        var jobId = scheduler.Do(async _ =>
        {
            started.SetResult();
            await release.Task;
        });

        await started.Task.WaitAsync(TimeSpan.FromSeconds(5));
        var waitTask = scheduler.WaitForCompletionAsync(jobId);
        Assert.False(waitTask.IsCompleted);

        release.SetResult();
        await waitTask.WaitAsync(TimeSpan.FromSeconds(5));

        Assert.Equal(YatkJobState.Succeeded, scheduler.GetJob(jobId)?.State);
    }

    // 失敗したジョブでも待機処理自体は正常に完了することを確認する。
    [Fact]
    public async Task WaitForCompletionAsync_CompletesForFailedJob()
    {
        await using var scheduler = new YatkScheduler();

        var jobId = scheduler.Do(_ => throw new InvalidOperationException("失敗"));

        await scheduler.WaitForCompletionAsync(jobId).WaitAsync(TimeSpan.FromSeconds(5));

        Assert.Equal(YatkJobState.Failed, scheduler.GetJob(jobId)?.State);
    }

    // キャンセルされたジョブを完了まで待機できることを確認する。
    [Fact]
    public async Task WaitForCompletionAsync_CompletesForCanceledJob()
    {
        var started = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var scheduler = new YatkScheduler();

        var jobId = scheduler.Do(async cancellationToken =>
        {
            started.SetResult();
            await Task.Delay(Timeout.InfiniteTimeSpan, cancellationToken);
        });

        await started.Task.WaitAsync(TimeSpan.FromSeconds(5));
        Assert.True(scheduler.Cancel(jobId));
        await scheduler.WaitForCompletionAsync(jobId).WaitAsync(TimeSpan.FromSeconds(5));

        Assert.Equal(YatkJobState.Canceled, scheduler.GetJob(jobId)?.State);
    }

    // 待機側のキャンセルがジョブ本体をキャンセルしないことを確認する。
    [Fact]
    public async Task WaitForCompletionAsync_CancellationOnlyCancelsWaiter()
    {
        var started = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        using var cancellationTokenSource = new CancellationTokenSource();
        await using var scheduler = new YatkScheduler();

        var jobId = scheduler.Do(async _ =>
        {
            started.SetResult();
            await release.Task;
        });

        await started.Task.WaitAsync(TimeSpan.FromSeconds(5));
        var waitTask = scheduler.WaitForCompletionAsync(jobId, cancellationTokenSource.Token);
        cancellationTokenSource.Cancel();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => waitTask);
        Assert.Equal(YatkJobState.Running, scheduler.GetJob(jobId)?.State);

        release.SetResult();
        await scheduler.WaitForCompletionAsync(jobId).WaitAsync(TimeSpan.FromSeconds(5));
    }

    // 未登録ジョブを待機すると例外になることを確認する。
    [Fact]
    public async Task WaitForCompletionAsync_ThrowsForUnknownJob()
    {
        await using var scheduler = new YatkScheduler();

        await Assert.ThrowsAsync<KeyNotFoundException>(
            () => scheduler.WaitForCompletionAsync(new YatkJobId(Guid.NewGuid())));
    }

    private sealed class TestJob : YatkJobBase
    {
        public TaskCompletionSource Completed { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        protected override Task ExecuteAsync(YatkJobContext context, CancellationToken cancellationToken)
        {
            Completed.SetResult();
            return Task.CompletedTask;
        }
    }

    private static void UpdateMaximum(ref int maximumObserved, int current)
    {
        while (true)
        {
            var observed = Volatile.Read(ref maximumObserved);
            if (observed >= current || Interlocked.CompareExchange(ref maximumObserved, current, observed) == observed)
            {
                return;
            }
        }
    }

}
