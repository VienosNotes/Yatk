# Yatk

Yatk は、アプリケーション内で非同期ジョブを FIFO 順に実行するための、軽量な .NET ジョブスケジューラです。

バックグラウンド処理を一定の並列数に抑えたい場合や、処理の状態・進捗・キャンセルをまとめて扱いたい場合に利用します。永続化、分散実行、再試行ポリシーなどは v1 の対象外です。

## v1 のコンセプト

- ジョブは一度だけ投入・実行するオブジェクトとして扱う
- 待機中のジョブは FIFO 順で開始する
- 最大並列数を守り、実行中にも変更できる
- キャンセルは協調的に行い、ジョブ本体へ `CancellationToken` で通知する
- 状態の確認には読み取り専用のスナップショットを使う
- 完了済みジョブの保持数を制限し、長時間稼働時の参照保持を抑える

対象フレームワークは .NET 10 です。

## 主な機能

- ラムダ式または `YatkJobBase` 派生クラスによるジョブ投入
- FIFO 実行と最大並列数の設定・動的変更
- ジョブごとの状態取得、一覧取得、完了待機
- 待機中ジョブの取り消しと、実行中ジョブへの協調キャンセル要求
- 進捗値・状態メッセージの報告
- `JobChanged` による状態遷移通知
- `Drain` / `Cancel` を選べる停止処理
- 完了済みジョブの保持上限

## 基本的な使い方

```csharp
using Yatk;

await using var scheduler = new YatkScheduler(maxConcurrency: 2);

var jobId = scheduler.Do(async cancellationToken =>
{
    await Task.Delay(TimeSpan.FromSeconds(1), cancellationToken);
}, name: "データ取得");

var wasKnown = await scheduler.WaitForCompletionAsync(jobId);
if (wasKnown)
{
    var job = scheduler.GetJob(jobId);
    Console.WriteLine(job?.State); // Succeeded
}
```

`WaitForCompletionAsync` は、ジョブの終了を待てた場合に `true` を返します。未登録または完了済み保持上限によって削除された ID の場合は `false` を返します。

## 進捗と状態の報告

`YatkJobContext` を受け取るオーバーロードでは、ジョブ本体から進捗と状態メッセージを報告できます。

```csharp
var jobId = scheduler.Do(async (context, cancellationToken) =>
{
    context.SetStatusMessage("準備中");
    context.ReportProgress(0.1);

    await Task.Delay(500, cancellationToken);

    context.SetStatusMessage("完了");
    context.ReportProgress(1.0);
}, name: "インポート");
```

進捗は `0.0` から `1.0` の範囲です。ジョブ実行の完了後に保持された `YatkJobContext` を利用しても、進捗・メッセージは更新されません。

## 状態の監視とキャンセル

`JobChanged` は状態遷移ごとに通知されます。通知は `Queued`、`Running`、`Succeeded` のような状態遷移順に配送され、イベント引数の `Snapshot` がその通知時点の状態を表します。

```csharp
scheduler.JobChanged += (_, eventArgs) =>
{
    var snapshot = eventArgs.Snapshot;
    Console.WriteLine($"{snapshot.Name}: {snapshot.State}");
};

var jobId = scheduler.Do(async cancellationToken =>
{
    await Task.Delay(Timeout.InfiniteTimeSpan, cancellationToken);
}, name: "中断可能な処理");

var requested = scheduler.Cancel(jobId);
```

待機中のジョブはただちに `Canceled` になり、待機キューから取り除かれます。実行中のジョブは `CancelRequested` になり、渡された `CancellationToken` がキャンセルされます。ジョブ本体はトークンを監視して速やかに終了してください。

完了済みジョブを保持しない設定では、完了イベントを受け取る時点でも `GetJob(eventArgs.JobId)` が `null` の場合があります。その場合も `eventArgs.Snapshot` を利用できます。

## 設定と停止

```csharp
await using var scheduler = new YatkScheduler(new YatkSchedulerOptions
{
    MaxConcurrency = 4,
    MaxRetainedCompletedJobs = 100,
});

// 実行中にも並列数を変更できる。
scheduler.SetMaxConcurrency(2);

// 待機中を含む全ジョブの完了を待つ。
await scheduler.StopAsync(YatkShutdownMode.Drain);

// または、待機中をキャンセルし、実行中へキャンセルを要求する。
// await scheduler.StopAsync(YatkShutdownMode.Cancel);
```

`MaxRetainedCompletedJobs` は完了済みジョブを保持する件数です。`0` を指定すると、完了と同時にスケジューラから削除されます。削除されたジョブは `GetJob` で取得できず、`Cancel` と `WaitForCompletionAsync` は `false` を返します。

## リッチジョブ

処理をクラスとして表現したい場合は `YatkJobBase` を継承し、`ExecuteAsync` を実装して `Enqueue` します。

```csharp
public sealed class CleanupJob : YatkJobBase
{
    public CleanupJob()
        : base("クリーンアップ")
    {
    }

    protected override async Task ExecuteAsync(
        YatkJobContext context,
        CancellationToken cancellationToken)
    {
        context.SetStatusMessage("削除中");
        await Task.Delay(100, cancellationToken);
    }
}

var jobId = scheduler.Enqueue(new CleanupJob());
```

詳細な仕様と実装進捗は [docs/v1-spec.md](docs/v1-spec.md) を参照してください。
