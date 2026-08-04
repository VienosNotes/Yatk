namespace Yatk;

/// <summary>
/// スケジューラの停止方法を表します。
/// </summary>
public enum YatkShutdownMode
{
    /// <summary>
    /// 待機中ジョブを含むすべてのジョブの完了を待機します。
    /// </summary>
    Drain,

    /// <summary>
    /// 待機中ジョブをキャンセルし、実行中ジョブへキャンセルを要求します。
    /// </summary>
    Cancel,
}
