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

    /// <summary>
    /// スケジューラが保持する完了済みジョブの最大数を取得または設定します。
    /// 0 を指定すると、完了済みジョブを保持しません。
    /// </summary>
    public int MaxRetainedCompletedJobs { get; init; } = 1000;

    /// <summary>
    /// 時刻の取得と遅延実行の待機に使う <see cref="System.TimeProvider"/> を取得または設定します。
    /// 既定値は <see cref="TimeProvider.System"/> です。
    /// </summary>
    public TimeProvider TimeProvider { get; init; } = TimeProvider.System;
}
