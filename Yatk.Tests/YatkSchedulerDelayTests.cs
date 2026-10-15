using System.Collections.Concurrent;
using System.Runtime.CompilerServices;
using Microsoft.Extensions.Time.Testing;

namespace Yatk.Tests;

/// <summary>
/// 遅延実行と <see cref="TimeProvider"/> の注入に関するテストです。
/// </summary>
public sealed class YatkSchedulerDelayTests
{
    private static readonly TimeSpan Timeout = TimeSpan.FromSeconds(5);

    // 時刻を進める前は実行されず、NotBefore まで進めると実行されることを確認する。
    [Fact]
    public async Task DelayedJob_RunsOnlyAfterNotBefore()
    {
        var time = new FakeTimeProvider();
        var start = time.GetUtcNow();
        var executed = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var scheduler = new YatkScheduler(timeProvider: time);

        var jobId = scheduler.Do(_ =>
        {
            executed.SetResult();
            return Task.CompletedTask;
        }, delay: TimeSpan.FromSeconds(60));

        var delayed = scheduler.GetJob(jobId);
        Assert.NotNull(delayed);
        Assert.Equal(YatkJobState.Delayed, delayed.State);
        Assert.Equal(start + TimeSpan.FromSeconds(60), delayed.NotBefore);
        Assert.Null(delayed.QueuedAt);

        time.Advance(TimeSpan.FromSeconds(60) - TimeSpan.FromMilliseconds(1));
        Assert.Equal(YatkJobState.Delayed, scheduler.GetJob(jobId)?.State);
        Assert.False(executed.Task.IsCompleted);

        time.Advance(TimeSpan.FromMilliseconds(1));
        await executed.Task.WaitAsync(Timeout);
        Assert.True(await scheduler.WaitForCompletionAsync(jobId).WaitAsync(Timeout));

        var completed = scheduler.GetJob(jobId);
        Assert.NotNull(completed);
        Assert.Equal(YatkJobState.Succeeded, completed.State);
        Assert.Equal(start + TimeSpan.FromSeconds(60), completed.NotBefore);
    }

    // 遅延待ちのジョブが実行枠を消費せず、後から投入した通常のジョブが先に実行されることを確認する。
    [Fact]
    public async Task DelayedJob_DoesNotConsumeConcurrency()
    {
        var time = new FakeTimeProvider();
        var normalExecuted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var delayedExecuted = false;
        await using var scheduler = new YatkScheduler(maxConcurrency: 1, timeProvider: time);

        scheduler.Do(_ =>
        {
            delayedExecuted = true;
            return Task.CompletedTask;
        }, delay: TimeSpan.FromSeconds(10));
        scheduler.Do(_ =>
        {
            normalExecuted.SetResult();
            return Task.CompletedTask;
        });

        await normalExecuted.Task.WaitAsync(Timeout);
        Assert.False(delayedExecuted);
    }

    // 遅延待ちのジョブが NotBefore の早い順、同時刻なら投入順に待機キューへ加わることを確認する。
    [Fact]
    public async Task DelayedJobs_AreQueuedInNotBeforeThenSubmissionOrder()
    {
        var time = new FakeTimeProvider();
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var blockerStarted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var started = new ConcurrentQueue<string>();
        var queued = new ConcurrentQueue<string?>();
        await using var scheduler = new YatkScheduler(maxConcurrency: 1, timeProvider: time);
        scheduler.JobChanged += (_, eventArgs) =>
        {
            if (eventArgs.Snapshot.State == YatkJobState.Queued)
            {
                queued.Enqueue(eventArgs.Snapshot.Name);
            }
        };

        var blockerId = scheduler.Do(async _ =>
        {
            blockerStarted.SetResult();
            await release.Task;
        }, "blocker");
        await blockerStarted.Task.WaitAsync(Timeout);

        var jobIds = new List<YatkJobId>
        {
            SubmitRecordingJob(scheduler, started, "A", TimeSpan.FromSeconds(30)),
            SubmitRecordingJob(scheduler, started, "B", TimeSpan.FromSeconds(10)),
            SubmitRecordingJob(scheduler, started, "C", TimeSpan.FromSeconds(10)),
            SubmitRecordingJob(scheduler, started, "D", TimeSpan.FromSeconds(20)),
        };

        // 一度に時刻を進め、複数のジョブの期限が同じタイマーの発火で到来するようにする。
        time.Advance(TimeSpan.FromSeconds(30));
        release.SetResult();

        foreach (var jobId in jobIds)
        {
            Assert.True(await scheduler.WaitForCompletionAsync(jobId).WaitAsync(Timeout));
        }

        Assert.Equal(["blocker", "B", "C", "D", "A"], queued);
        Assert.Equal(["B", "C", "D", "A"], started);
        Assert.True(await scheduler.WaitForCompletionAsync(blockerId).WaitAsync(Timeout));
    }

    // 実行可能になったジョブが、その時点の待機キューの末尾に加わることを確認する。
    [Fact]
    public async Task ReadyJob_JoinsTailOfQueueAtReadyTime()
    {
        var time = new FakeTimeProvider();
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var blockerStarted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var started = new ConcurrentQueue<string>();
        await using var scheduler = new YatkScheduler(maxConcurrency: 1, timeProvider: time);

        scheduler.Do(async _ =>
        {
            blockerStarted.SetResult();
            await release.Task;
        });
        await blockerStarted.Task.WaitAsync(Timeout);

        var delayedId = SubmitRecordingJob(scheduler, started, "delayed", TimeSpan.FromSeconds(10));
        var beforeId = SubmitRecordingJob(scheduler, started, "before", TimeSpan.Zero);
        time.Advance(TimeSpan.FromSeconds(10));
        var afterId = SubmitRecordingJob(scheduler, started, "after", TimeSpan.Zero);
        release.SetResult();

        Assert.True(await scheduler.WaitForCompletionAsync(afterId).WaitAsync(Timeout));
        Assert.True(await scheduler.WaitForCompletionAsync(delayedId).WaitAsync(Timeout));
        Assert.True(await scheduler.WaitForCompletionAsync(beforeId).WaitAsync(Timeout));
        Assert.Equal(["before", "delayed", "after"], started);
    }

    // 実行可能になった High のジョブが、待機中の Normal より先に開始することを確認する。
    [Fact]
    public async Task ReadyHighJob_StartsBeforeQueuedNormalJob()
    {
        var time = new FakeTimeProvider();
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var blockerStarted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var started = new ConcurrentQueue<string>();
        await using var scheduler = new YatkScheduler(maxConcurrency: 1, timeProvider: time);

        scheduler.Do(async _ =>
        {
            blockerStarted.SetResult();
            await release.Task;
        });
        await blockerStarted.Task.WaitAsync(Timeout);

        var normalId = SubmitRecordingJob(scheduler, started, "normal", TimeSpan.Zero);
        var highId = SubmitRecordingJob(scheduler, started, "high", TimeSpan.FromSeconds(10), YatkJobPriority.High);
        time.Advance(TimeSpan.FromSeconds(10));
        release.SetResult();

        Assert.True(await scheduler.WaitForCompletionAsync(normalId).WaitAsync(Timeout));
        Assert.True(await scheduler.WaitForCompletionAsync(highId).WaitAsync(Timeout));
        Assert.Equal(["high", "normal"], started);
    }

    // 実行可能になった Immediate のジョブが、実行枠に空きがなくても開始することを確認する。
    [Fact]
    public async Task ReadyImmediateJob_StartsBeyondConcurrencyLimit()
    {
        var time = new FakeTimeProvider();
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var blockerStarted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var immediateExecuted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var scheduler = new YatkScheduler(maxConcurrency: 1, timeProvider: time);

        scheduler.Do(async _ =>
        {
            blockerStarted.SetResult();
            await release.Task;
        });
        await blockerStarted.Task.WaitAsync(Timeout);

        scheduler.Do(_ =>
        {
            immediateExecuted.SetResult();
            return Task.CompletedTask;
        }, priority: YatkJobPriority.Immediate, delay: TimeSpan.FromSeconds(10));

        try
        {
            time.Advance(TimeSpan.FromSeconds(10));
            await immediateExecuted.Task.WaitAsync(Timeout);
        }
        finally
        {
            release.TrySetResult();
        }
    }

    // 0 以下の遅延が、遅延を指定しない投入と同じ動作になることを確認する。
    [Theory]
    [InlineData(0)]
    [InlineData(-1000)]
    public async Task NonPositiveDelay_BehavesLikeNormalSubmission(int delayMilliseconds)
    {
        var states = new ConcurrentQueue<YatkJobState>();
        var succeededNotified = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var scheduler = new YatkScheduler();
        scheduler.JobChanged += (_, eventArgs) =>
        {
            states.Enqueue(eventArgs.Snapshot.State);
            Assert.Null(eventArgs.Snapshot.NotBefore);
            if (eventArgs.Snapshot.State == YatkJobState.Succeeded)
            {
                succeededNotified.TrySetResult();
            }
        };

        scheduler.Do(_ => Task.CompletedTask, delay: TimeSpan.FromMilliseconds(delayMilliseconds));

        await succeededNotified.Task.WaitAsync(Timeout);
        Assert.Equal([YatkJobState.Queued, YatkJobState.Running, YatkJobState.Succeeded], states);
    }

    // 現在時刻に遅延を加えて範囲を超える場合に拒否し、ジョブを登録しないことを確認する。
    [Fact]
    public async Task Delay_RejectsOverflowingNotBefore()
    {
        await using var scheduler = new YatkScheduler(timeProvider: new FakeTimeProvider());
        var job = new NoOpJob();

        Assert.Throws<ArgumentOutOfRangeException>(() => scheduler.Enqueue(job, delay: TimeSpan.MaxValue));

        Assert.Empty(scheduler.GetJobs());
        Assert.Equal(YatkJobState.Created, job.State);
    }

    // 状態変更イベントが Delayed、Queued、Running の順に発生し、時刻が TimeProvider から記録されることを確認する。
    [Fact]
    public async Task DelayedJob_RaisesEventsAndRecordsTimesFromTimeProvider()
    {
        var time = new FakeTimeProvider();
        var start = time.GetUtcNow();
        var snapshots = new ConcurrentQueue<YatkJobSnapshot>();
        var succeededNotified = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var scheduler = new YatkScheduler(timeProvider: time);
        scheduler.JobChanged += (_, eventArgs) =>
        {
            snapshots.Enqueue(eventArgs.Snapshot);
            if (eventArgs.Snapshot.State == YatkJobState.Succeeded)
            {
                succeededNotified.TrySetResult();
            }
        };

        scheduler.Do((context, _) =>
        {
            context.ReportProgress(0.5);
            return Task.CompletedTask;
        }, delay: TimeSpan.FromSeconds(10));

        // 期限よりも後まで一度に進め、QueuedAt が NotBefore ではなく実際に加わった時刻になることを確認する。
        time.Advance(TimeSpan.FromSeconds(15));
        await succeededNotified.Task.WaitAsync(Timeout);

        var received = snapshots.ToArray();
        Assert.Equal(
            [YatkJobState.Delayed, YatkJobState.Queued, YatkJobState.Running, YatkJobState.Running, YatkJobState.Succeeded],
            received.Select(snapshot => snapshot.State));
        Assert.All(received, snapshot => Assert.Equal(start + TimeSpan.FromSeconds(10), snapshot.NotBefore));

        var readyAt = start + TimeSpan.FromSeconds(15);
        Assert.Null(received[0].QueuedAt);
        Assert.Equal(start, received[0].ChangedAt);
        Assert.Equal(readyAt, received[1].QueuedAt);
        Assert.Equal(readyAt, received[1].ChangedAt);
        Assert.Equal(readyAt, received[2].StartedAt);
        Assert.Equal(readyAt, received[3].ChangedAt);
        Assert.Equal(readyAt, received[4].CompletedAt);
    }

    // GetJobs で遅延待ちのジョブを取得できることを確認する。
    [Fact]
    public async Task GetJobs_IncludesDelayedJobs()
    {
        await using var scheduler = new YatkScheduler(timeProvider: new FakeTimeProvider());

        var jobId = scheduler.Do(_ => Task.CompletedTask, delay: TimeSpan.FromSeconds(10));

        var snapshot = Assert.Single(scheduler.GetJobs());
        Assert.Equal(jobId, snapshot.JobId);
        Assert.Equal(YatkJobState.Delayed, snapshot.State);
    }

    // 遅延待ちのジョブを取り消すと完了待機が終わり、時刻を進めても実行されないことを確認する。
    [Fact]
    public async Task Cancel_DelayedJobCompletesWaitAndNeverRuns()
    {
        var time = new FakeTimeProvider();
        var executed = false;
        await using var scheduler = new YatkScheduler(timeProvider: time);

        var jobId = scheduler.Do(_ =>
        {
            executed = true;
            return Task.CompletedTask;
        }, delay: TimeSpan.FromSeconds(10));
        var waitTask = scheduler.WaitForCompletionAsync(jobId);

        time.Advance(TimeSpan.FromSeconds(3));
        Assert.True(scheduler.Cancel(jobId));
        Assert.True(await waitTask.WaitAsync(Timeout));

        var canceled = scheduler.GetJob(jobId);
        Assert.NotNull(canceled);
        Assert.Equal(YatkJobState.Canceled, canceled.State);
        Assert.Null(canceled.QueuedAt);
        Assert.Equal(time.GetUtcNow(), canceled.CompletedAt);

        time.Advance(TimeSpan.FromSeconds(10));
        Assert.Equal(YatkJobState.Canceled, scheduler.GetJob(jobId)?.State);
        Assert.False(executed);
        Assert.False(scheduler.Cancel(jobId));
    }

    // 先頭の遅延待ちのジョブを取り消すと、タイマーが次のジョブの期限に合わせ直されることを確認する。
    [Fact]
    public async Task Cancel_FirstDelayedJobReschedulesForNextJob()
    {
        var time = new FakeTimeProvider();
        var secondExecuted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var scheduler = new YatkScheduler(timeProvider: time);

        var firstId = scheduler.Do(_ => Task.CompletedTask, delay: TimeSpan.FromSeconds(10));
        var secondId = scheduler.Do(_ =>
        {
            secondExecuted.SetResult();
            return Task.CompletedTask;
        }, delay: TimeSpan.FromSeconds(20));

        Assert.True(scheduler.Cancel(firstId));
        time.Advance(TimeSpan.FromSeconds(10));
        Assert.Equal(YatkJobState.Delayed, scheduler.GetJob(secondId)?.State);

        time.Advance(TimeSpan.FromSeconds(10));
        await secondExecuted.Task.WaitAsync(Timeout);
    }

    // タイマーに一度に設定できる上限を超える遅延でも、期限まで待ってから実行されることを確認する。
    [Fact]
    public async Task DelayedJob_LongerThanTimerLimitRunsAtNotBefore()
    {
        var time = new FakeTimeProvider();
        var executed = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var scheduler = new YatkScheduler(timeProvider: time);

        var jobId = scheduler.Do(_ =>
        {
            executed.SetResult();
            return Task.CompletedTask;
        }, delay: TimeSpan.FromDays(60));

        time.Advance(TimeSpan.FromDays(50));
        Assert.Equal(YatkJobState.Delayed, scheduler.GetJob(jobId)?.State);

        time.Advance(TimeSpan.FromDays(10));
        await executed.Task.WaitAsync(Timeout);
    }

    // システム時刻が戻っても、待ち時間の経過後に遅れずに実行されることを確認する。
    [Fact]
    public async Task DelayedJob_IsNotPostponedWhenSystemClockMovesBack()
    {
        var time = new ClockAdjustableTimeProvider();
        var start = time.GetUtcNow();
        var executed = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var scheduler = new YatkScheduler(timeProvider: time);

        var jobId = scheduler.Do(_ =>
        {
            executed.SetResult();
            return Task.CompletedTask;
        }, delay: TimeSpan.FromSeconds(60));

        time.AdjustClock(TimeSpan.FromHours(-1));
        time.Advance(TimeSpan.FromSeconds(60));
        await executed.Task.WaitAsync(Timeout);

        // NotBefore は投入時点の時計で見た予定時刻のため、時計が戻ると QueuedAt が NotBefore より前になる。
        var snapshot = scheduler.GetJob(jobId);
        Assert.NotNull(snapshot);
        Assert.Equal(start + TimeSpan.FromSeconds(60), snapshot.NotBefore);
        Assert.Equal(start + TimeSpan.FromSeconds(60) - TimeSpan.FromHours(1), snapshot.QueuedAt);
    }

    // システム時刻が進んでも、待ち時間が経過していないジョブは実行されないことを確認する。
    [Fact]
    public async Task DelayedJob_IsNotAdvancedWhenSystemClockMovesForward()
    {
        var time = new ClockAdjustableTimeProvider();
        var firstExecuted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var scheduler = new YatkScheduler(timeProvider: time);

        scheduler.Do(_ =>
        {
            firstExecuted.SetResult();
            return Task.CompletedTask;
        }, delay: TimeSpan.FromSeconds(10));
        var secondId = scheduler.Do(_ => Task.CompletedTask, delay: TimeSpan.FromSeconds(60));

        // 先のジョブのタイマーが発火した時点で、後のジョブの NotBefore は時計の上では過ぎている。
        time.AdjustClock(TimeSpan.FromHours(1));
        time.Advance(TimeSpan.FromSeconds(10));
        await firstExecuted.Task.WaitAsync(Timeout);

        Assert.Equal(YatkJobState.Delayed, scheduler.GetJob(secondId)?.State);
    }

    // どちらの停止方法でも、遅延待ちのジョブを待たずに取り消すことを確認する。
    [Theory]
    [InlineData(YatkShutdownMode.Drain)]
    [InlineData(YatkShutdownMode.Cancel)]
    public async Task StopAsync_CancelsDelayedJobsWithoutWaiting(YatkShutdownMode mode)
    {
        var time = new FakeTimeProvider();
        var executed = false;
        var scheduler = new YatkScheduler(timeProvider: time);

        var jobId = scheduler.Do(_ =>
        {
            executed = true;
            return Task.CompletedTask;
        }, delay: TimeSpan.FromSeconds(10));

        await scheduler.StopAsync(mode).WaitAsync(Timeout);

        Assert.Equal(YatkJobState.Canceled, scheduler.GetJob(jobId)?.State);
        time.Advance(TimeSpan.FromSeconds(10));
        Assert.False(executed);
    }

    // 停止後に遅延を指定して投入すると拒否されることを確認する。
    [Fact]
    public async Task Do_WithDelayAfterStopThrows()
    {
        var scheduler = new YatkScheduler(timeProvider: new FakeTimeProvider());
        await scheduler.StopAsync(YatkShutdownMode.Drain);

        Assert.Throws<ObjectDisposedException>(
            () => scheduler.Do(_ => Task.CompletedTask, delay: TimeSpan.FromSeconds(10)));
    }

    // 設定の TimeProvider に null を指定すると拒否されることを確認する。
    [Fact]
    public void Constructor_RejectsNullTimeProviderInOptions()
    {
        Assert.Throws<ArgumentNullException>(
            () => new YatkScheduler(new YatkSchedulerOptions { TimeProvider = null! }));
    }

    // 終了したラムダジョブのデリゲートへの参照が解放されることを確認する。
    [Fact]
    public async Task CompletedLambdaJob_ReleasesDelegate()
    {
        await using var scheduler = new YatkScheduler();

        var (jobId, captured) = SubmitJobCapturingObject(scheduler, TimeSpan.Zero);
        Assert.True(await scheduler.WaitForCompletionAsync(jobId).WaitAsync(Timeout));

        AssertCollected(captured);
        Assert.Equal(YatkJobState.Succeeded, scheduler.GetJob(jobId)?.State);
    }

    // 遅延待ちのまま取り消したラムダジョブのデリゲートへの参照が解放されることを確認する。
    [Fact]
    public async Task CanceledDelayedLambdaJob_ReleasesDelegate()
    {
        await using var scheduler = new YatkScheduler(timeProvider: new FakeTimeProvider());

        var (jobId, captured) = SubmitJobCapturingObject(scheduler, TimeSpan.FromSeconds(10));
        Assert.True(scheduler.Cancel(jobId));

        AssertCollected(captured);
        Assert.Equal(YatkJobState.Canceled, scheduler.GetJob(jobId)?.State);
    }

    // 取り消しと時刻の到来が同時に起きても、実行は 0 回か 1 回で、状態が整合することを確認する。
    [Fact]
    public async Task CancelAndReadyRace_RunsAtMostOnceWithConsistentState()
    {
        for (var iteration = 0; iteration < 200; iteration++)
        {
            var time = new FakeTimeProvider();
            var executions = 0;
            await using var scheduler = new YatkScheduler(timeProvider: time);

            var jobId = scheduler.Do(_ =>
            {
                Interlocked.Increment(ref executions);
                return Task.CompletedTask;
            }, delay: TimeSpan.FromSeconds(1));

            using var barrier = new Barrier(2);
            var advanceTask = Task.Run(() =>
            {
                barrier.SignalAndWait();
                time.Advance(TimeSpan.FromSeconds(1));
            });
            var cancelTask = Task.Run(() =>
            {
                barrier.SignalAndWait();
                return scheduler.Cancel(jobId);
            });

            await Task.WhenAll(advanceTask, cancelTask).WaitAsync(Timeout);
            Assert.True(await scheduler.WaitForCompletionAsync(jobId).WaitAsync(Timeout));

            var state = scheduler.GetJob(jobId)?.State;
            if (Volatile.Read(ref executions) == 0)
            {
                Assert.Equal(YatkJobState.Canceled, state);
            }
            else
            {
                // ジョブはトークンを監視しないため、実行された場合はキャンセル要求後でも成功で終わる。
                Assert.Equal(1, Volatile.Read(ref executions));
                Assert.Equal(YatkJobState.Succeeded, state);
            }
        }
    }

    // 停止開始と時刻の到来が同時に起きても、実行は 0 回か 1 回で、状態が整合することを確認する。
    [Fact]
    public async Task StopAndReadyRace_RunsAtMostOnceWithConsistentState()
    {
        for (var iteration = 0; iteration < 200; iteration++)
        {
            var time = new FakeTimeProvider();
            var executions = 0;
            var scheduler = new YatkScheduler(timeProvider: time);

            var jobId = scheduler.Do(_ =>
            {
                Interlocked.Increment(ref executions);
                return Task.CompletedTask;
            }, delay: TimeSpan.FromSeconds(1));

            using var barrier = new Barrier(2);
            var advanceTask = Task.Run(() =>
            {
                barrier.SignalAndWait();
                time.Advance(TimeSpan.FromSeconds(1));
            });
            var stopTask = Task.Run(() =>
            {
                barrier.SignalAndWait();
                return scheduler.StopAsync(YatkShutdownMode.Drain);
            });

            await Task.WhenAll(advanceTask, stopTask).WaitAsync(Timeout);

            var state = scheduler.GetJob(jobId)?.State;
            if (Volatile.Read(ref executions) == 0)
            {
                Assert.Equal(YatkJobState.Canceled, state);
            }
            else
            {
                Assert.Equal(1, Volatile.Read(ref executions));
                Assert.Equal(YatkJobState.Succeeded, state);
            }
        }
    }

    private static YatkJobId SubmitRecordingJob(
        YatkScheduler scheduler,
        ConcurrentQueue<string> started,
        string name,
        TimeSpan delay,
        YatkJobPriority priority = YatkJobPriority.Normal)
    {
        return scheduler.Do(_ =>
        {
            started.Enqueue(name);
            return Task.CompletedTask;
        }, name, priority, delay);
    }

    // 捕捉対象のオブジェクトへの強い参照がテストメソッドのローカル変数に残らないよう、インライン化を禁止する。
    [MethodImpl(MethodImplOptions.NoInlining)]
    private static (YatkJobId JobId, WeakReference Captured) SubmitJobCapturingObject(YatkScheduler scheduler, TimeSpan delay)
    {
        var payload = new byte[1024];
        var jobId = scheduler.Do(_ =>
        {
            GC.KeepAlive(payload);
            return Task.CompletedTask;
        }, delay: delay);

        return (jobId, new WeakReference(payload));
    }

    private static void AssertCollected(WeakReference reference)
    {
        for (var attempt = 0; attempt < 10 && reference.IsAlive; attempt++)
        {
            GC.Collect();
            GC.WaitForPendingFinalizers();
        }

        Assert.False(reference.IsAlive);
    }

    /// <summary>
    /// 単調な時計を変えずに、システム時刻だけをずらせる <see cref="TimeProvider"/> です。
    /// </summary>
    /// <remarks>
    /// <see cref="FakeTimeProvider"/> は 2 つの時計を同じだけ進めるため、時計の補正を再現するために使う。
    /// 単調な時計とタイマーは内部の <see cref="FakeTimeProvider"/> に任せる。
    /// </remarks>
    private sealed class ClockAdjustableTimeProvider : TimeProvider
    {
        private readonly FakeTimeProvider inner = new();
        private TimeSpan clockOffset;

        public override long TimestampFrequency => inner.TimestampFrequency;

        public override DateTimeOffset GetUtcNow()
        {
            return inner.GetUtcNow() + clockOffset;
        }

        public override long GetTimestamp()
        {
            return inner.GetTimestamp();
        }

        public override ITimer CreateTimer(TimerCallback callback, object? state, TimeSpan dueTime, TimeSpan period)
        {
            return inner.CreateTimer(callback, state, dueTime, period);
        }

        public void AdjustClock(TimeSpan offset)
        {
            clockOffset += offset;
        }

        public void Advance(TimeSpan delta)
        {
            inner.Advance(delta);
        }
    }

    private sealed class NoOpJob : YatkJobBase
    {
        protected override Task ExecuteAsync(YatkJobContext context, CancellationToken cancellationToken)
        {
            return Task.CompletedTask;
        }
    }
}
