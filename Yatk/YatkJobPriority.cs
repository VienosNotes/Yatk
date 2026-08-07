namespace Yatk;

/// <summary>
/// ジョブ投入時の実行優先度を表します。
/// </summary>
public enum YatkJobPriority
{
    /// <summary>通常の FIFO 順で実行します。</summary>
    Normal,

    /// <summary>通常の待機ジョブより先に、最大並列度の範囲内で実行します。</summary>
    High,

    /// <summary>最大並列度にかかわらず直ちに実行します。</summary>
    Immediate,
}
