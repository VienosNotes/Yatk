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
    /// ジョブを非同期で実行します。
    /// </summary>
    /// <param name="cancellationToken">将来のキャンセル機能のために渡されるトークンです。</param>
    /// <returns>実行処理を表すタスクです。</returns>
    protected abstract Task ExecuteAsync(CancellationToken cancellationToken);

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

    internal void MarkRunning(DateTimeOffset timestamp)
    {
        lock (syncRoot)
        {
            state = YatkJobState.Running;
            startedAt = timestamp;
        }
    }

    internal void MarkSucceeded(DateTimeOffset timestamp)
    {
        lock (syncRoot)
        {
            state = YatkJobState.Succeeded;
            completedAt = timestamp;
        }
    }

    internal void MarkFailed(DateTimeOffset timestamp, Exception error)
    {
        lock (syncRoot)
        {
            state = YatkJobState.Failed;
            completedAt = timestamp;
            exception = error;
        }
    }

    internal Task ExecuteInternalAsync(CancellationToken cancellationToken)
    {
        return ExecuteAsync(cancellationToken);
    }

    internal YatkJobSnapshot CreateSnapshot()
    {
        lock (syncRoot)
        {
            return new YatkJobSnapshot(
                JobId,
                Name,
                state,
                Progress: null,
                StatusMessage: null,
                queuedAt,
                startedAt,
                completedAt,
                exception);
        }
    }
}
