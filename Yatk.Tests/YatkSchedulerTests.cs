using System.Collections.Concurrent;

namespace Yatk.Tests;

public sealed class YatkSchedulerTests
{
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

    [Fact]
    public async Task Enqueue_ExecutesRichJob()
    {
        var job = new TestJob();
        await using var scheduler = new YatkScheduler();

        scheduler.Enqueue(job);

        await job.Completed.Task.WaitAsync(TimeSpan.FromSeconds(5));
    }

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

    private sealed class TestJob : YatkJobBase
    {
        public TaskCompletionSource Completed { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        protected override Task ExecuteAsync(CancellationToken cancellationToken)
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
