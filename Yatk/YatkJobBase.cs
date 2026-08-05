namespace Yatk;

/// <summary>
/// リッチジョブを実装するための基底クラスです。
/// </summary>
public abstract class YatkJobBase
{
    private readonly object syncRoot = new();
    private YatkJobState state;
    private DateTimeOffset? queuedAt;
    private DateTimeOffset? startedAt;
    private DateTimeOffset? completedAt;
    private Exception? exception;
    private double? progress;
    private string? statusMessage;
    private readonly TaskCompletionSource completion = new(TaskCreationOptions.RunContinuationsAsynchronously);

    /// <summary>
    /// ジョブを初期化します。
    /// </summary>
    /// <param name="name">ジョブを識別する任意の名前です。</param>
    protected YatkJobBase(string? name = null)
    {
        JobId = new YatkJobId(Guid.NewGuid());
        Name = name;
        state = YatkJobState.Created;
    }

    /// <summary>
    /// ジョブを識別する ID を取得します。
    /// </summary>
    public YatkJobId JobId { get; }

    /// <summary>
    /// ジョブの現在の状態を取得します。
    /// </summary>
    public YatkJobState State
    {
        get
        {
            lock (syncRoot)
            {
                return state;
            }
        }
    }

    /// <summary>
    /// ジョブを識別する任意の名前を取得します。
    /// </summary>
    public string? Name { get; }

    /// <summary>
    /// 最後に報告された進捗を取得します。
    /// </summary>
    public double? Progress
    {
        get
        {
            lock (syncRoot)
            {
                return progress;
            }
        }
    }

    /// <summary>
    /// 最後に設定された状態メッセージを取得します。
    /// </summary>
    public string? StatusMessage
    {
        get
        {
            lock (syncRoot)
            {
                return statusMessage;
            }
        }
    }

    /// <summary>
    /// ジョブを非同期で実行します。
    /// </summary>
    /// <param name="context">進捗や状態メッセージを報告するためのコンテキストです。</param>
    /// <param name="cancellationToken">ジョブのキャンセル通知を受け取るトークンです。</param>
    /// <returns>実行処理を表すタスクです。</returns>
    protected abstract Task ExecuteAsync(YatkJobContext context, CancellationToken cancellationToken);

    internal bool TryMarkQueued(DateTimeOffset timestamp)
    {
        lock (syncRoot)
        {
            if (state != YatkJobState.Created)
            {
                return false;
            }

            state = YatkJobState.Queued;
            queuedAt = timestamp;
            return true;
        }
    }

    internal bool TryMarkRunning(DateTimeOffset timestamp)
    {
        lock (syncRoot)
        {
            if (state != YatkJobState.Queued)
            {
                return false;
            }

            state = YatkJobState.Running;
            startedAt = timestamp;
            return true;
        }
    }

    internal bool TryMarkCancelRequested()
    {
        lock (syncRoot)
        {
            if (state != YatkJobState.Running)
            {
                return false;
            }

            state = YatkJobState.CancelRequested;
            return true;
        }
    }

    internal bool TryMarkQueuedCanceled(DateTimeOffset timestamp)
    {
        lock (syncRoot)
        {
            if (state != YatkJobState.Queued)
            {
                return false;
            }

            state = YatkJobState.Canceled;
            completedAt = timestamp;
            completion.TrySetResult();
            return true;
        }
    }

    internal bool TryMarkCanceledAfterCancellation(DateTimeOffset timestamp)
    {
        lock (syncRoot)
        {
            if (state != YatkJobState.CancelRequested)
            {
                return false;
            }

            state = YatkJobState.Canceled;
            completedAt = timestamp;
            completion.TrySetResult();
            return true;
        }
    }

    internal void MarkSucceeded(DateTimeOffset timestamp)
    {
        lock (syncRoot)
        {
            state = YatkJobState.Succeeded;
            completedAt = timestamp;
            completion.TrySetResult();
        }
    }

    internal void MarkFailed(DateTimeOffset timestamp, Exception error)
    {
        lock (syncRoot)
        {
            state = YatkJobState.Failed;
            completedAt = timestamp;
            exception = error;
            completion.TrySetResult();
        }
    }

    internal async Task ExecuteInternalAsync(CancellationToken cancellationToken)
    {
        var context = new YatkJobContext(this);

        try
        {
            await ExecuteAsync(context, cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            context.Invalidate();
        }
    }

    internal void SetProgress(double value)
    {
        lock (syncRoot)
        {
            progress = value;
        }
    }

    internal void SetStatusMessage(string? message)
    {
        lock (syncRoot)
        {
            statusMessage = message;
        }
    }

    internal Task WaitForCompletionAsync(CancellationToken cancellationToken)
    {
        return completion.Task.WaitAsync(cancellationToken);
    }

    internal YatkJobSnapshot CreateSnapshot()
    {
        lock (syncRoot)
        {
            return new YatkJobSnapshot(
                JobId,
                Name,
                state,
                progress,
                statusMessage,
                queuedAt,
                startedAt,
                completedAt,
                exception);
        }
    }
}
