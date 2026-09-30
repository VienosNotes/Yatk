namespace Yatk;

/// <summary>
/// ある時点におけるジョブの読み取り専用状態です。
/// </summary>
public sealed record YatkJobSnapshot(
    YatkJobId JobId,
    string? Name,
    YatkJobState State,
    double? Progress,
    string? StatusMessage,
    DateTimeOffset? QueuedAt,
    DateTimeOffset? StartedAt,
    DateTimeOffset? CompletedAt,
    Exception? Exception)
{
    /// <summary>
    /// ジョブ投入時に指定された優先度を取得します。
    /// </summary>
    public YatkJobPriority Priority { get; init; } = YatkJobPriority.Normal;

    /// <summary>
    /// 状態、進捗、状態メッセージのいずれかが最後に変更された時刻を取得します。
    /// </summary>
    /// <remarks>
    /// <see cref="YatkScheduler.JobChanged"/> で受け取ったスナップショットでは、その通知が表す変更の発生時刻です。
    /// </remarks>
    public DateTimeOffset ChangedAt { get; init; }
}
