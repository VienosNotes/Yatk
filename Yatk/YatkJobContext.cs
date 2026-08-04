namespace Yatk;

/// <summary>
/// ジョブの実行中に状態を報告するためのコンテキストです。
/// </summary>
public sealed class YatkJobContext
{
    private readonly YatkJobBase job;

    internal YatkJobContext(YatkJobBase job)
    {
        this.job = job;
    }

    /// <summary>
    /// 実行中のジョブ ID を取得します。
    /// </summary>
    public YatkJobId JobId => job.JobId;

    /// <summary>
    /// ジョブの進捗を報告します。
    /// </summary>
    /// <param name="progress">0.0 から 1.0 までの進捗値です。</param>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="progress"/> が 0.0 から 1.0 の範囲外の場合に発生します。</exception>
    public void ReportProgress(double progress)
    {
        if (double.IsNaN(progress) || double.IsInfinity(progress) || progress < 0.0 || progress > 1.0)
        {
            throw new ArgumentOutOfRangeException(nameof(progress));
        }

        job.SetProgress(progress);
    }

    /// <summary>
    /// ジョブの状態メッセージを設定します。
    /// </summary>
    /// <param name="message">設定するメッセージ。<see langword="null"/> を指定するとメッセージをクリアします。</param>
    public void SetStatusMessage(string? message)
    {
        job.SetStatusMessage(message);
    }
}
