namespace Yatk;

/// <summary>
/// ジョブを識別する値です。
/// </summary>
/// <param name="Value">内部で使用する GUID 値です。</param>
public readonly record struct YatkJobId(Guid Value);
