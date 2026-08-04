namespace Yatk;

/// <summary>
/// ジョブ状態の変更を通知するイベント引数です。
/// </summary>
public sealed class YatkJobChangedEventArgs : EventArgs
{
    /// <summary>
    /// イベント引数を初期化します。
    /// </summary>
    /// <param name="snapshot">変更後のジョブスナップショットです。</param>
    public YatkJobChangedEventArgs(YatkJobSnapshot snapshot)
    {
        Snapshot = snapshot;
    }

    /// <summary>
    /// 状態が変更されたジョブの ID を取得します。
    /// </summary>
    public YatkJobId JobId => Snapshot.JobId;

    /// <summary>
    /// 変更後のジョブスナップショットを取得します。
    /// </summary>
    public YatkJobSnapshot Snapshot { get; }
}
