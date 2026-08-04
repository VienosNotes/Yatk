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
    Exception? Exception);
