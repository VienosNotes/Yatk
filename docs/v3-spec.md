# Yatk v3 遅延実行仕様

## この文書について

この文書は、Yatk v2 に遅延実行機能と `TimeProvider` の注入を追加するための仕様書です。
記載のない動作は `v1-spec.md` と `v2-spec.md` を引き継ぎます。

元の要望は GitHub Issue #3「遅延実行機能の追加」です。

v3 では、次のことをできるようにします。

- ジョブを「指定した待ち時間の経過後に実行可能になる」ものとして投入する
- 時刻が来る前のジョブを、スケジューラ上で観測・取り消しできるようにする
- 時刻の取得と待機に使う `TimeProvider` を差し替えられるようにする

再試行ポリシー、重複投入の抑止、タグやグループ単位の一括取り消し、永続化、周期実行（cron）は v3 の対象外です。

## 進捗管理

各実装項目では、以下の値を使用して進捗を記録します。

- ⬜ Not started
- 🚧 In progress
- 🟨 Partially implemented
- ✅ Implemented
- ⚠️ Blocked

仕様変更や判断が必要な事項は、仕様本文を直接変更せず、対象項目の `Notes` に `Proposal:` として記録します。

## 初期状態

Status: ✅ Implemented

Notes:

Implementation:
- `YatkJobState.Delayed` と、`Do`、`Enqueue` の引数 `delay` を追加した。
- `YatkSchedulerOptions.TimeProvider` を追加し、最大並列度を受け取るコンストラクタを `(int maxConcurrency = 1, TimeProvider? timeProvider = null)` に置き換えた。
- `YatkJobSnapshot.NotBefore` を追加した。
- 遅延待ちのジョブを `SortedSet` に `NotBefore`、投入順の順で保持し、`TimeProvider.CreateTimer` で作成したタイマー 1 つで先頭の期限を待つようにした。タイマーは最初の遅延投入時に作成する。
- タイマーの発火時は `GetUtcNow()` で期限を判定し直し、期限が来たジョブを先頭からまとめて待機キューへ加えるようにした。
- タイマーに設定する待ち時間を、1 ミリ秒以上、0xFFFFFFFE ミリ秒以下に制限した。
- 停止開始時に、遅延待ちのジョブを取り消し、タイマーをロックの外で破棄するようにした。
- すべての時刻を `TimeProvider.GetUtcNow()` から取得するようにした。`YatkJobContext` の進捗・状態メッセージの報告時刻も含む。
- ラムダジョブのデリゲートへの参照を、実行開始時、または実行されずに終了状態になった時点で解放するようにした。完了の通知より前に解放される。
- README に遅延実行と `TimeProvider` の節を追加した。

Proposal:
- 「内部実装要件」に、タイマーのコールバックの再入への対処を追加したい。`FakeTimeProvider.GetUtcNow()` は、期限の来たタイマーのコールバックを呼び出し元のスレッドで同期的に呼ぶことがある（別スレッドの `Advance` との競合時や、`AutoAdvanceAmount` の使用時）。スケジューラのロック内でこれが起きると、列挙中の遅延待ちの領域が変更されたり、`StartAvailableJobs` の途中で別のジョブが開始されて最大並列度を超えたりする。実装では、コールバックがロックを保持したスレッドで呼ばれた場合に、`ThreadPool` で処理し直すようにした。

Tests:
- 遅延実行、順序、優先度との組み合わせ、0 以下と範囲外の遅延、状態とイベント、時刻の記録、キャンセル、タイマーの上限を超える遅延、停止、参照の解放、競合条件のテストを `YatkSchedulerDelayTests` に追加した。
- テストプロジェクトに `Microsoft.Extensions.TimeProvider.Testing` を追加した。
- 既存のテストを含む 68 件が成功した。

## 互換性の方針

Yatk の利用者はすべて自社製品であり、更新時には常に再ビルドします。
そのため v3 では、バイナリ互換性は維持しません。ソース互換性も、あるべき設計を優先し、必要であれば破壊的変更を許容します。

ただし、遅延を指定しない投入の動作は v2 と変えないでください。

破壊的変更の一覧は「利用側への影響」に記載します。

## 目的

利用者が次の要求を、スケジューラの外にタイマーを持たずに表現できるようにします。

1. 失敗した処理を、一定時間後に再実行したい。
2. 外部リソースの準備を待ってから処理したい。

利用者がタイマーを持つ場合と比べて、次の点を改善します。

- 時刻が来る前のジョブも `GetJob`、`GetJobs`、`JobChanged` で状態を追える。
- 時刻が来る前のジョブを `Cancel` で取り消せ、停止時の後始末もスケジューラが行う。
- テストで時刻を進めることで、実時間を待たずに決定的な検証ができる。

## 公開 API

### `YatkJobState`

時刻が来るのを待っている状態として `Delayed` を追加します。

```csharp
public enum YatkJobState
{
    Created,
    Delayed,
    Queued,
    Running,
    CancelRequested,
    Succeeded,
    Failed,
    Canceled,
}
```

- `Delayed`: 投入済みで、実行可能になる時刻を待っている。待機キューには入っておらず、実行枠も消費しない。

状態の遷移の順に合わせて、`Created` の直後に置きます。これにより、`Queued` 以降の数値は v2 から 1 ずつ増えます。
利用側の DB には状態を文字列で保存しているため、数値の変化は問題にならない前提とします。

### 投入 API

既存の投入 API に、省略可能な引数 `delay` を追加します。

```csharp
public YatkJobId Enqueue(
    YatkJobBase job,
    YatkJobPriority priority = YatkJobPriority.Normal,
    TimeSpan delay = default);

public YatkJobId Do(
    Func<CancellationToken, Task> action,
    string? name = null,
    YatkJobPriority priority = YatkJobPriority.Normal,
    TimeSpan delay = default);

public YatkJobId Do(
    Func<YatkJobContext, CancellationToken, Task> action,
    string? name = null,
    YatkJobPriority priority = YatkJobPriority.Normal,
    TimeSpan delay = default);
```

- `delay` が `TimeSpan.Zero` 以下の場合は、v2 と同じ投入になる。`Delayed` を経由せず、`Delayed` のイベントも発生しない。
- `delay` が正の場合は `Delayed` で登録し、投入時刻に `delay` を加えた時刻（`NotBefore`）以降に実行可能にする。
- 投入時刻に `delay` を加えて `DateTimeOffset` の範囲を超える場合は、`ArgumentOutOfRangeException` で拒否する。

時刻を直接指定する引数（`notBefore: DateTimeOffset`）は追加しません。待ち時間と時刻の両方を受け付けると、両方指定された場合の扱いが必要になるためです。
時刻を指定したい利用者は、スケジューラと同じ `TimeProvider` で待ち時間に換算してください。

優先度と遅延は独立した指定です。`YatkJobBase` のインスタンス自体には、優先度と同様に遅延を保持させません。

### `YatkSchedulerOptions`

時刻の取得と待機に使う `TimeProvider` を追加します。

```csharp
public TimeProvider TimeProvider { get; init; } = TimeProvider.System;
```

`null` を指定した場合は、スケジューラの生成時に `ArgumentNullException` を発生させてください。

### `YatkScheduler` のコンストラクタ

最大並列度を受け取る既存のコンストラクタを、次のものに置き換えます。追加ではなく置き換えです。
既存のコンストラクタを残すと、`new YatkScheduler()` の呼び出しが曖昧になるためです。

```csharp
public YatkScheduler(int maxConcurrency = 1, TimeProvider? timeProvider = null);
```

`timeProvider` が `null` の場合は `TimeProvider.System` を使います。

`YatkSchedulerOptions` を受け取るコンストラクタは変更しません。

### `YatkJobSnapshot`

実行可能になる時刻を表すプロパティを追加します。既存の `Priority`、`ChangedAt` と同様に、位置引数ではなく init プロパティとして追加してください。

```csharp
/// 遅延を指定して投入された場合の、実行可能になる時刻を取得します。
public DateTimeOffset? NotBefore { get; init; }
```

- 遅延を指定して投入したジョブでは、`Delayed` の間もその後も同じ値を返す。
- 遅延を指定しなかった、または 0 以下の遅延を指定したジョブでは `null` を返す。

## 状態遷移

遅延を指定したジョブの状態遷移は次のとおりです。

```text
Created -> Delayed -> Queued -> Running -> Succeeded / Failed
              |                    |
              -> Canceled          -> CancelRequested -> Canceled / Succeeded / Failed
```

遅延を指定しないジョブの状態遷移は v2 と同じです。

### 時刻の記録

- `Delayed` への遷移時は、`QueuedAt`、`StartedAt`、`CompletedAt` のいずれも記録しない。`ChangedAt` には投入時刻を記録する。
- `Queued` への遷移時は、実際に待機キューへ加えた時刻を `QueuedAt` に記録する。この時刻は `NotBefore` 以降になる。
- `Delayed` から取り消した場合は、取り消した時刻を `CompletedAt` に記録する。`QueuedAt` は `null` のままとする。

v1 では、状態遷移のイベントの `ChangedAt` は、対応する `QueuedAt`、`StartedAt`、`CompletedAt` と同じ値になると定めています。
`Delayed` への遷移には対応する時刻がないため、この規則の例外とします。

すべての時刻は、スケジューラの `TimeProvider.GetUtcNow()` から取得してください。
`YatkJobContext` による進捗・状態メッセージの報告時刻も含みます。

## 実行可能になったときの扱い

`NotBefore` 以降になったジョブは、`Queued` へ遷移し、その時点で通常の投入と同じ扱いになります。

- `Normal` と `High` は、それぞれの優先度の待機キューの末尾に加える。
- `Immediate` は、v2 と同じく直ちに `Running` へ遷移する。最大並列度に空きがなくても開始する。
- 最大並列度、キャンセル、完了待機、保持上限の扱いは、通常のジョブと同じ。

### 順序

FIFO の順序は、投入した時刻ではなく、待機キューに加えた順で決まります。

- 遅延待ちのジョブより後に投入した通常のジョブは、遅延待ちのジョブより先に実行されうる。
- 複数の遅延待ちのジョブは、`NotBefore` の早い順に待機キューへ加える。
- `NotBefore` が同じ場合は、投入した順に待機キューへ加える。

時刻が来たジョブが複数ある場合は、上記の順序で、一つの同期範囲の中でまとめて待機キューへ加えてください。
タイマーのコールバックが呼ばれる順序に、ジョブ間の順序を依存させてはいけません。

`Queued` への遷移は、ジョブごとに `JobChanged` で通知します。通知の順序は、待機キューに加えた順序と一致させてください。

### 精度

実行可能になる時刻は「`NotBefore` 以降」であれば十分であり、厳密な精度は求めません。
タイマーの解像度やスレッドの割り当てにより、`NotBefore` から遅れて `Queued` になることがあります。

## キャンセル

`Delayed` のジョブに対する `Cancel(jobId)` は、次のように動作します。

- 遅延待ちの領域から物理的に削除する。
- 直ちに `Canceled` へ遷移し、`true` を返す。
- `WaitForCompletionAsync` の待機を完了させる。
- その後に `NotBefore` になっても、ジョブを実行しない。

`Queued` 以降に遷移したジョブのキャンセルは、v2 と同じです。

### 時刻の到来との競合

取り消しと時刻の到来が同時に起きた場合は、スケジューラの同期範囲で先に成立した操作を採用します。

- 取り消しが先に成立した場合: `Canceled` になり、ジョブ本体は実行されない。
- 時刻の到来が先に成立した場合: `Queued` になり、その後の取り消しは `Queued` または `Running` に対する通常のキャンセルとして扱う。

どちらの場合も、ジョブ本体の実行は 0 回または 1 回で、状態は終了状態のいずれか一つに収束しなければなりません。

## Graceful Shutdown

停止開始時には、`Drain` と `Cancel` のどちらのモードでも、`Delayed` のジョブを実行せずに `Canceled` へ遷移させます。

- `Drain` は、遅延待ちのジョブの `NotBefore` まで待たない。
- `Drain` で待つのは、停止開始時点で `Queued` または実行中のジョブだけとする。
- 停止開始後は、遅延を指定した投入も含めて新規投入を拒否する（v1 と同じ）。

停止開始と時刻の到来が競合した場合は、同期範囲で先に成立した操作を採用します。
停止開始が先に成立した場合、そのジョブは `Canceled` になります。

停止開始時に、遅延待ちのためのタイマーを破棄してください。

`Drain` で停止するときの待ち時間の上限は、v3 では追加しません。
必要な場合は、ホスト側のシャットダウンのタイムアウトと `StopAsync` の `cancellationToken`、`StopAsync(YatkShutdownMode.Cancel)` を組み合わせて対応してください。

## 参照の解放

ラムダジョブのデリゲート（クロージャ）への参照は、ジョブが終了状態になった時点で解放してください。
完了済みジョブの保持期間中に、クロージャが保持する大きなオブジェクトや機密情報がメモリに残り続けないようにするためです。

- 対象は、`Succeeded`、`Failed`、`Canceled` のすべての終了状態とする。`Delayed` または `Queued` から取り消した場合も含む。
- スナップショット、完了待機、保持上限の動作は変えない。
- `YatkJobBase` の派生クラスが保持するフィールドは、利用者の管理とし、v3 では解放の対象にしない。

## 内部実装要件

具体的なデータ構造は限定しませんが、少なくとも次の性質を満たしてください。

- 遅延待ちのジョブが数百件程度あっても、待機のためにジョブごとにスレッドを占有しない。
- タイマーは、スケジューラの `TimeProvider.CreateTimer` で作成する。
- タイマーは、最も早い `NotBefore` に合わせて一つだけ持つ方法を推奨する。ジョブの追加、取り消し、時刻の到来のたびに、次の期限に合わせて設定し直す。
- `TimeProvider` によっては、タイマーに設定できる待ち時間に上限がある（`TimeProvider.System` では約 49.7 日）。上限を超える待ち時間は上限で区切って設定し、タイマーが発火したら残りの時間で設定し直す。
- 遅延待ちのジョブを、`NotBefore` と投入順で順序付けて保持する。取り消し時に、対象を O(log n) 以下で削除できる。
- 状態遷移、遅延待ちの領域、待機キュー、実行中数、停止状態を、v1 と同じ同期方針で保護する。
- ロック中に、ジョブ本体、イベントハンドラ、タイマーの破棄を待つ処理を実行しない。
- 遅延を指定しない投入では、タイマーの作成や遅延待ちの領域の操作を行わない。

タイマーのコールバックは、`TimeProvider` が選んだスレッドで実行されます。
そのため `JobChanged` のハンドラも、タイマーのスレッドから呼ばれることがあります。
`FakeTimeProvider.Advance` を使った場合は、`Advance` を呼んだスレッドの中でハンドラが実行されます。

## 外部依存

ライブラリ本体には、新しい外部依存を追加しません。`TimeProvider` は .NET の基本クラスライブラリに含まれています。

テストプロジェクトには、`FakeTimeProvider` を使うために `Microsoft.Extensions.TimeProvider.Testing` を追加して構いません。

## 利用側への影響

v2 からの破壊的変更と、利用側で必要になる対応は次のとおりです。

| 変更 | 利用側への影響 | 必要な対応 |
|---|---|---|
| `YatkJobState.Delayed` の追加 | `YatkJobState` を網羅的に扱う `switch` 式で CS8509 の警告が出る。`Queued` 以降の数値が 1 ずつ増える。 | `Delayed` の分岐を追加する。状態を文字列で保存している箇所では、表示名の対応に `Delayed` を追加する。 |
| `Do`、`Enqueue` のシグネチャ変更 | バイナリ互換性がなくなる。ソースの呼び出しはそのままコンパイルできる。 | 再ビルドする。 |
| 最大並列度を受け取るコンストラクタの置き換え | バイナリ互換性がなくなる。ソースの呼び出しはそのままコンパイルできる。 | 再ビルドする。 |
| `Delayed` の間は `QueuedAt` が `null` | 遅延を使った場合だけ影響する。「投入済みなら `QueuedAt` は非 null」という前提が成り立たなくなる。 | 遅延を使う箇所で `QueuedAt` を参照する場合は、`null` を考慮する。 |
| `Drain` で遅延待ちのジョブを取り消す | 遅延を使った場合だけ影響する。 | 停止後も実行したいジョブには、遅延を指定しない。 |

遅延を指定しない利用者では、実行時の動作は v2 から変わりません。

## テスト

時刻に依存する検証では、実時間の経過を待たずに `FakeTimeProvider.Advance` で時刻を進めてください。
実行枠の占有と解放は、v2 と同様に `TaskCompletionSource` などで制御してください。

### 互換性

- 遅延を指定しない既存のテストがすべて成功する。
- 0 以下の遅延を指定した投入が、遅延を指定しない投入と同じ動作になる。`Delayed` のイベントは発生しない。

### 遅延実行

- 時刻を進める前は実行されず、`NotBefore` まで進めると実行される。
- `NotBefore` の直前まで進めても実行されない。
- 遅延待ちのジョブは、最大並列度の上限を消費しない。
- 複数の遅延待ちのジョブが、`NotBefore` の早い順に実行される。
- `NotBefore` が同じ遅延待ちのジョブが、投入した順に実行される。
- 遅延待ちのジョブより後に投入した通常のジョブが、先に実行される。
- 実行可能になったジョブが、その時点の待機キューの末尾に加わる。
- 実行可能になった `High` のジョブが、待機中の `Normal` より先に開始する。
- 実行可能になった `Immediate` のジョブが、最大並列度に空きがなくても開始する。
- 投入時刻に遅延を加えて範囲を超える場合に、`ArgumentOutOfRangeException` を発生させる。

### 状態とイベント

- `GetJob` と `GetJobs` で、`Delayed` の状態と `NotBefore` を取得できる。
- `JobChanged` が、`Delayed`、`Queued`、`Running` の順に発生する。
- `Delayed` の間は `QueuedAt` が `null` で、`Queued` への遷移時に `NotBefore` 以降の時刻が記録される。
- 記録されるすべての時刻が、指定した `TimeProvider` から取得される。

### キャンセル

- 遅延待ちのジョブの取り消しで `Canceled` になり、`Cancel` が `true` を返す。
- 遅延待ちのジョブの取り消しで、`WaitForCompletionAsync` が完了する。
- 取り消した後に時刻を進めても、ジョブが実行されない。

### 停止

- 遅延待ちのジョブがある状態で `Drain` すると、時刻を進めずに停止が完了し、そのジョブは `Canceled` になる。
- 遅延待ちのジョブがある状態で `Cancel` すると、そのジョブは `Canceled` になる。
- 停止後に遅延を指定して投入すると、`ObjectDisposedException` を発生させる。

### 参照の解放

- 終了したラムダジョブのデリゲートへの参照が解放される。`WeakReference` などで確認する。
- `Delayed` から取り消したラムダジョブのデリゲートへの参照が解放される。

### 競合条件

- 遅延待ちのジョブの取り消しと、時刻の到来の競合。実行は 0 回または 1 回で、状態が整合する。
- 停止開始と、時刻の到来の競合。
- 遅延待ちのジョブの投入と、停止開始の競合。

## README 更新要件

実装時には README に次を追記してください。

- 遅延実行の使い方と、`delay` が 0 以下の場合の動作。
- `Delayed` の状態と、`NotBefore` の意味。
- FIFO の順序が、投入した順ではなく待機キューに加えた順で決まること。
- `Drain` を含むどちらの停止モードでも、遅延待ちのジョブは取り消されること。
- `TimeProvider` の指定方法と、`FakeTimeProvider` を使ったテストの例。
- `JobChanged` のハンドラが、タイマーのスレッドから呼ばれることがあること。
- `ChangedAt` と `QueuedAt` などの時刻が一致する規則の、`Delayed` に関する例外。

## 完了条件

以下をすべて満たした時点で、v3 の遅延実行機能を完了とします。

- 遅延を指定しない既存のコードが、v2 と同じ動作をする。
- 遅延を指定したジョブが、`NotBefore` 以降に通常のジョブとして実行される。
- 遅延待ちのジョブを観測、取り消しでき、停止時に取り消される。
- 時刻の取得と待機が、指定した `TimeProvider` で行われる。
- 終了したラムダジョブのデリゲートへの参照が解放される。
- 公開 API に XML ドキュメントコメントがある。
- README と実装進捗が更新されている。
- 追加したテストと既存のテストが、すべて成功する。

## 将来検討

以下は v3 の対象外とします。

- 時刻を直接指定する投入 API（`notBefore`）。
- 遅延待ちのジョブの `NotBefore` の変更。
- `Drain` で停止するときの待ち時間の上限。
- 再試行ポリシー。
- 重複投入の抑止。
- タグやグループ単位の一括取り消し。
- 周期実行（cron）。
- 永続化。
