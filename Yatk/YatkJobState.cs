namespace Yatk;

/// <summary>
/// ジョブの実行状態を表します。
/// </summary>
public enum YatkJobState
{
    /// <summary>未投入です。</summary>
    Created,

    /// <summary>実行待ちです。</summary>
    Queued,

    /// <summary>実行中です。</summary>
    Running,

    /// <summary>キャンセルが要求されています。</summary>
    CancelRequested,

    /// <summary>正常に完了しました。</summary>
    Succeeded,

    /// <summary>例外により失敗しました。</summary>
    Failed,

    /// <summary>キャンセルされました。</summary>
    Canceled,
}
