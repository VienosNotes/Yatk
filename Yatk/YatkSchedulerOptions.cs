namespace Yatk;

/// <summary>
/// スケジューラの初期設定です。
/// </summary>
public sealed class YatkSchedulerOptions
{
    /// <summary>
    /// 同時に実行するジョブの最大数を取得または設定します。
    /// </summary>
    public int MaxConcurrency { get; init; } = 1;
}
