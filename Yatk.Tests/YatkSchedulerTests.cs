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
        await scheduler.WaitForCompletionAsync(jobId).WaitAsync(TimeSpan.FromSeconds(5));

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

    // 未登録ジョブを待機すると false が返ることを確認する。
    [Fact]
    public async Task WaitForCompletionAsync_ReturnsFalseForUnknownJob()
    {
        await using var scheduler = new YatkScheduler();

        var result = await scheduler.WaitForCompletionAsync(new YatkJobId(Guid.NewGuid()));

        Assert.False(result);
    }

    // 状態変更後のスナップショットをイベントで取得できることを確認する。
    [Fact]
    public async Task JobChanged_RaisesForLifecycleChanges()
    {
        var states = new ConcurrentQueue<YatkJobState>();
        var started = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var scheduler = new YatkScheduler();
        scheduler.JobChanged += (_, eventArgs) => states.Enqueue(eventArgs.Snapshot.State);

        var jobId = scheduler.Do(async _ =>
        {
            started.SetResult();
            await release.Task;
        });

        await started.Task.WaitAsync(TimeSpan.FromSeconds(5));
        release.SetResult();
        await scheduler.WaitForCompletionAsync(jobId).WaitAsync(TimeSpan.FromSeconds(5));

        Assert.Contains(YatkJobState.Queued, states);
        Assert.Contains(YatkJobState.Running, states);
        Assert.Contains(YatkJobState.Succeeded, states);
    }

    // イベントハンドラの例外でジョブ実行が停止しないことを確認する。
    [Fact]
    public async Task JobChanged_HandlerExceptionDoesNotStopScheduler()
    {
        var completed = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var scheduler = new YatkScheduler();
        scheduler.JobChanged += (_, _) => throw new InvalidOperationException("イベント失敗");

        var jobId = scheduler.Do(_ =>
        {
            completed.SetResult();
            return Task.CompletedTask;
        });

        await completed.Task.WaitAsync(TimeSpan.FromSeconds(5));
        await scheduler.WaitForCompletionAsync(jobId).WaitAsync(TimeSpan.FromSeconds(5));

        Assert.Equal(YatkJobState.Succeeded, scheduler.GetJob(jobId)?.State);
    }

    // Drain 停止で待機中ジョブを含む全ジョブが完了することを確認する。
    [Fact]
    public async Task StopAsync_DrainCompletesQueuedJobs()
    {
        var firstStarted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var releaseFirst = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var secondCompleted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var scheduler = new YatkScheduler();

        var firstJobId = scheduler.Do(async _ =>
        {
            firstStarted.SetResult();
            await releaseFirst.Task;
        });
        var secondJobId = scheduler.Do(_ =>
        {
            secondCompleted.SetResult();
            return Task.CompletedTask;
        });

        await firstStarted.Task.WaitAsync(TimeSpan.FromSeconds(5));
        var stopTask = scheduler.StopAsync(YatkShutdownMode.Drain);
        Assert.Throws<ObjectDisposedException>(() => scheduler.Do(_ => Task.CompletedTask));

        releaseFirst.SetResult();
        await stopTask.WaitAsync(TimeSpan.FromSeconds(5));
        await secondCompleted.Task.WaitAsync(TimeSpan.FromSeconds(5));

        Assert.Equal(YatkJobState.Succeeded, scheduler.GetJob(firstJobId)?.State);
        Assert.Equal(YatkJobState.Succeeded, scheduler.GetJob(secondJobId)?.State);
    }

    // Cancel 停止で待機中・実行中ジョブがキャンセルされることを確認する。
    [Fact]
    public async Task StopAsync_CancelCancelsQueuedAndRunningJobs()
    {
        var firstStarted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var secondExecuted = false;
        await using var scheduler = new YatkScheduler();

        var firstJobId = scheduler.Do(async cancellationToken =>
        {
            firstStarted.SetResult();
            await Task.Delay(Timeout.InfiniteTimeSpan, cancellationToken);
        });
        var secondJobId = scheduler.Do(_ =>
        {
            secondExecuted = true;
            return Task.CompletedTask;
        });

        await firstStarted.Task.WaitAsync(TimeSpan.FromSeconds(5));
        await scheduler.StopAsync(YatkShutdownMode.Cancel).WaitAsync(TimeSpan.FromSeconds(5));

        Assert.Equal(YatkJobState.Canceled, scheduler.GetJob(firstJobId)?.State);
        Assert.Equal(YatkJobState.Canceled, scheduler.GetJob(secondJobId)?.State);
        Assert.False(secondExecuted);
    }

    // 複数回の停止要求が同じ停止処理の完了を待機できることを確認する。
    [Fact]
    public async Task StopAsync_CanBeCalledMultipleTimes()
    {
        var started = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var scheduler = new YatkScheduler();

        scheduler.Do(async _ =>
        {
            started.SetResult();
            await release.Task;
        });

        await started.Task.WaitAsync(TimeSpan.FromSeconds(5));
        var firstStopTask = scheduler.StopAsync(YatkShutdownMode.Drain);
        var secondStopTask = scheduler.StopAsync(YatkShutdownMode.Cancel);

        release.SetResult();
        await Task.WhenAll(firstStopTask, secondStopTask).WaitAsync(TimeSpan.FromSeconds(5));
    }

    // オプションで初期最大並列度を指定できることを確認する。
    [Fact]
    public async Task Constructor_AppliesMaxConcurrencyFromOptions()
    {
        await using var scheduler = new YatkScheduler(new YatkSchedulerOptions { MaxConcurrency = 2 });

        Assert.Equal(2, scheduler.MaxConcurrency);
    }

    // 最大並列度を増やすと待機中ジョブが追加で開始されることを確認する。
    [Fact]
    public async Task SetMaxConcurrency_IncreaseStartsQueuedJobs()
    {
        var firstStarted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var started = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var startCount = 0;
        await using var scheduler = new YatkScheduler(maxConcurrency: 1);

        for (var index = 0; index < 3; index++)
        {
            scheduler.Do(async _ =>
            {
                var current = Interlocked.Increment(ref startCount);
                if (current == 1)
                {
                    firstStarted.SetResult();
                }

                if (current == 3)
                {
                    started.SetResult();
                }

                await release.Task;
            });
        }

        await firstStarted.Task.WaitAsync(TimeSpan.FromSeconds(5));
        Assert.Equal(1, Volatile.Read(ref startCount));
        scheduler.SetMaxConcurrency(3);
        await started.Task.WaitAsync(TimeSpan.FromSeconds(5));

        release.SetResult();
    }

    // 最大並列度を減らしても実行中ジョブを止めず、次の開始を抑制することを確認する。
    [Fact]
    public async Task SetMaxConcurrency_DecreaseDelaysNewJobs()
    {
        var allInitialJobsStarted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var firstFinished = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var releaseFirst = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var releaseOthers = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var fourthStarted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var initialStartCount = 0;
        await using var scheduler = new YatkScheduler(maxConcurrency: 3);

        for (var index = 0; index < 3; index++)
        {
            var jobIndex = index;
            scheduler.Do(async _ =>
            {
                if (Interlocked.Increment(ref initialStartCount) == 3)
                {
                    allInitialJobsStarted.SetResult();
                }

                if (jobIndex == 0)
                {
                    await releaseFirst.Task;
                    firstFinished.SetResult();
                }
                else
                {
                    await releaseOthers.Task;
                }
            });
        }

        await allInitialJobsStarted.Task.WaitAsync(TimeSpan.FromSeconds(5));
        scheduler.SetMaxConcurrency(1);
        scheduler.Do(_ =>
        {
            fourthStarted.SetResult();
            return Task.CompletedTask;
        });

        releaseFirst.SetResult();
        await firstFinished.Task.WaitAsync(TimeSpan.FromSeconds(5));
        Assert.False(fourthStarted.Task.IsCompleted);

        releaseOthers.SetResult();
        await fourthStarted.Task.WaitAsync(TimeSpan.FromSeconds(5));
    }

    // 無効な最大並列度がコンストラクタと変更 API の両方で拒否されることを確認する。
    [Fact]
    public async Task SetMaxConcurrency_RejectsValuesBelowOne()
    {
        Assert.Throws<ArgumentOutOfRangeException>(() => new YatkScheduler(maxConcurrency: 0));
        Assert.Throws<ArgumentOutOfRangeException>(() => new YatkScheduler(new YatkSchedulerOptions { MaxConcurrency = 0 }));

        await using var scheduler = new YatkScheduler();
        Assert.Throws<ArgumentOutOfRangeException>(() => scheduler.SetMaxConcurrency(0));
    }

    // 複数スレッドから投入しても最大並列度を超えず全ジョブが実行されることを確認する。
    [Fact]
    public async Task Enqueue_IsThreadSafeUnderConcurrentCalls()
    {
        const int jobCount = 12;
        const int maxConcurrency = 3;
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var allStarted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var startedCount = 0;
        var runningCount = 0;
        var maximumObserved = 0;
        await using var scheduler = new YatkScheduler(maxConcurrency);

        var enqueueTasks = Enumerable.Range(0, jobCount).Select(_ => Task.Run(() =>
            scheduler.Do(async _ =>
            {
                var current = Interlocked.Increment(ref runningCount);
                UpdateMaximum(ref maximumObserved, current);
                if (Interlocked.Increment(ref startedCount) == maxConcurrency)
                {
                    allStarted.SetResult();
                }

                await release.Task;
                Interlocked.Decrement(ref runningCount);
            })));

        await Task.WhenAll(enqueueTasks);
        await allStarted.Task.WaitAsync(TimeSpan.FromSeconds(5));
        Assert.Equal(maxConcurrency, maximumObserved);

        release.SetResult();
        await scheduler.StopAsync(YatkShutdownMode.Drain).WaitAsync(TimeSpan.FromSeconds(5));
        Assert.Equal(jobCount, Volatile.Read(ref startedCount));
    }

    // 完了済みジョブが設定した保持件数を超えると最古のジョブから削除されることを確認する。
    [Fact]
    public async Task CompletedJobs_AreRemovedBeyondRetentionLimit()
    {
        await using var scheduler = new YatkScheduler(new YatkSchedulerOptions
        {
            MaxRetainedCompletedJobs = 1,
        });

        var firstJobId = scheduler.Do(_ => Task.CompletedTask);
        Assert.True(await scheduler.WaitForCompletionAsync(firstJobId).WaitAsync(TimeSpan.FromSeconds(5)));
        Assert.NotNull(scheduler.GetJob(firstJobId));

        var secondJobId = scheduler.Do(_ => Task.CompletedTask);
        Assert.True(await scheduler.WaitForCompletionAsync(secondJobId).WaitAsync(TimeSpan.FromSeconds(5)));

        Assert.Null(scheduler.GetJob(firstJobId));
        Assert.False(await scheduler.WaitForCompletionAsync(firstJobId));
        Assert.NotNull(scheduler.GetJob(secondJobId));
    }

    // 削除前に待機を開始した場合は、完了後にジョブが削除されても true で完了することを確認する。
    [Fact]
    public async Task WaitForCompletionAsync_CompletesWhenJobIsRemovedAfterWaitStarts()
    {
        var started = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var scheduler = new YatkScheduler(new YatkSchedulerOptions
        {
            MaxRetainedCompletedJobs = 0,
        });

        var jobId = scheduler.Do(async _ =>
        {
            started.SetResult();
            await release.Task;
        });

        await started.Task.WaitAsync(TimeSpan.FromSeconds(5));
        var waitTask = scheduler.WaitForCompletionAsync(jobId);
        release.SetResult();

        Assert.True(await waitTask.WaitAsync(TimeSpan.FromSeconds(5)));
        Assert.Null(scheduler.GetJob(jobId));
        Assert.False(await scheduler.WaitForCompletionAsync(jobId));
    }

    // 完了済みジョブの保持件数に負の値を指定できないことを確認する。
    [Fact]
    public void Constructor_RejectsNegativeCompletedJobRetentionLimit()
    {
        Assert.Throws<ArgumentOutOfRangeException>(() => new YatkScheduler(new YatkSchedulerOptions
        {
            MaxRetainedCompletedJobs = -1,
        }));
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
