# sqs-job-worker

[English](https://github.com/xmart-corp/sqs-job-worker/blob/main/README.md) | [日本語](https://github.com/xmart-corp/sqs-job-worker/blob/main/README.ja.md)

Amazon SQSのジョブを処理する、フレームワーク非依存の常駐ワーカーです。
boto3クライアントを渡すだけで動き、コアはWebフレームワークにもAPM SDKにもデプロイ環境にも依存しません。

## 特徴

- 1件ずつ受信して処理するシンプルな常駐ワーカー。並列度はプロセス(コンテナ)数に比例します
- 複数キューを重み付きランダム順でポーリング。優先度をつけつつ、重みの小さいキューも取り残されません
- ジョブ実行中は専用スレッドがvisibility timeoutを延長し続けるため、実行時間の読めないジョブでも二重配信されにくくなります
- SIGTERM / SIGINTでのグレースフルシャットダウン。処理中のジョブは完了させ、未処理のメッセージは即座にキューへ返します
- produce / poll / consumeの3フックを持つRackスタイルのミドルウェア
- New Relic・OpenTelemetry・Sentryのトレース伝搬と、ECSローリングデプロイ用のドレインを`contrib`として同梱
- structlogによる構造化ログ。相関フィールドはSQSメッセージ属性で運ばれ、コンシューマー側のログに自動で付与されます
- 標準キュー・FIFOキューの両対応

## インストール

```console
$ pip install sqs-job-worker
```

APM連携を使う場合は対応するextraを指定します。

```console
$ pip install "sqs-job-worker[newrelic]"
$ pip install "sqs-job-worker[otel]"
$ pip install "sqs-job-worker[sentry]"
```

Python 3.11以上が必要です。

## クイックスタート

論理名と物理キューを対応づけた`QueueGroup`を作ります。
プロデューサーとコンシューマーで同じ定義を共有してください。

```python
import boto3

from sqs_job_worker import QueueGroup

sqs = boto3.client("sqs")
queues = QueueGroup.build(boto_client=sqs, queues={"default": {"name": "my-app-jobs"}})
```

ジョブを投入します。

```python
queues.enqueue("default", "send_welcome_email", {"user_id": 42})
```

ワーカープロセスでは、`job_type`ごとのハンドラーを渡してループを起動します。

```python
def send_welcome_email(payload: dict) -> None:
    ...

queues.run_worker({"send_welcome_email": send_welcome_email})
```

`run_worker()`はシグナル処理込みのブロッキングループです。
ECSやKubernetesではこのプロセスをそのまま常駐サービスとして動かします。

メッセージは少なくとも1回配信されます。
同じメッセージが複数回届くこともあるため、ハンドラーは冪等に実装してください。

## メッセージ形式

メッセージボディは次のJSONです。
この形式に従えば、他の言語で書いたプロデューサーからも投入できます。

```json
{"job_type": "send_welcome_email", "payload": {"user_id": 42}}
```

- ボディをJSONとして解釈できないとき、`job_type`が文字列でないとき、`payload`がオブジェクトでないときは、リトライしても直らないためメッセージを削除します
- `job_type`に対応するハンドラーが登録されていない場合は、そのジョブを担当する別のワーカーに再配信されることを想定して、メッセージを削除せずに残します。短時間で再配信を繰り返さないよう、visibility timeoutを最低5分まで延長します。FIFOキューでは同じmessage groupの後続も同じ期間止まるため、未知の`job_type`はredrive policyでデッドレターキューへ移されるまで順序配信を停滞させます

相関フィールドとトレースヘッダーはボディではなくSQSメッセージ属性で運ばれます。

`enqueue()`では、FIFOキュー用の`message_group_id` / `message_deduplication_id`などSQSの送信オプションもそのまま指定できます。
標準キュー宛てなら`delay_seconds`で配信を遅らせることもできます。
FIFOキューはメッセージ単位の遅延に対応していません。

```python
queues.enqueue(
    "default",
    "recalculate_balance",
    {"account_id": "a-1"},
    message_group_id="a-1",
    message_deduplication_id="job-1",
)
```

## リトライと失敗

- ハンドラーが正常終了すると、メッセージは削除されます
- ハンドラーが例外を送出すると、メッセージは削除されずvisibility timeoutの経過後に再配信されます
- 本番キューではSQSのredrive policyとデッドレターキューを必ず設定し、ハンドラーの失敗、検証をすり抜けた不正メッセージ、未知の`job_type`が無制限に再試行されないようにしてください
- リトライしても成功する見込みがない場合は`NonRetryableError`を送出してください。メッセージは再配信されず、すぐに削除されます

```python
from sqs_job_worker import NonRetryableError

def sync_user(payload: dict) -> None:
    user = find_user(payload["user_id"])
    if user is None:
        raise NonRetryableError("user was deleted; retrying cannot succeed")
```

## 複数キューと重み付きポーリング

```python
queues = QueueGroup.build(
    boto_client=sqs,
    queues={
        "critical": {"name": "my-app-jobs-critical", "weight": 5},
        "default": {"name": "my-app-jobs", "weight": 1},
    },
)
```

- ワーカーは1サイクルごとに、重みに応じてキューの並び順をランダムに決め、その順に各キューをショートポーリングして、最初に見つかったメッセージを1件処理します
- `weight`はこの並び順の抽選に使う相対値です。上の例では`critical`が先頭に来やすくなりますが、どのキューも毎サイクル必ず確認されるため、重みの小さいキューが放置されることはありません
- どのキューも空のときは、先頭に並んだキューに対して`idle_wait_seconds`(デフォルト20秒)のロングポーリングを行い、メッセージの到着を待ちます

`name`の代わりに`url`でキューURLを直接指定することもできます。キューごとに細かく設定したい場合は、`SqsQueue`を自分で作って`QueueGroup`に渡します。

```python
from sqs_job_worker import QueueGroup, SqsQueue

heavy = SqsQueue(queue_url, boto_client=sqs, max_runtime_seconds=7200)
queues = QueueGroup({"heavy": {"queue": heavy, "weight": 1}})
```

## 長時間ジョブとvisibility timeout

- visibility timeoutを指定しない場合、ワーカー起動時にキューの設定値を取得して使います
- ジョブの実行中は専用スレッド(ハートビート)がvisibility timeoutの1/3の間隔で延長を繰り返します。キューのvisibility timeoutをジョブの最大実行時間に合わせて伸ばしておく必要はありません
- ジョブの実行時間が`max_runtime_seconds`(デフォルト3600秒)を超えるとハートビートは停止し、メッセージはSQSへ戻って再配信されます
- visibility timeoutの延長に失敗したジョブは、すでに別のワーカーへ再配信されている可能性があるため、正常終了してもメッセージを削除しません

## グレースフルシャットダウン

`run_worker()`はSIGTERM / SIGINTを受けると新しいメッセージの受信をやめ、処理中のジョブを完了させてから終了します。
もう一度シグナルを送ると即時終了します。
停止要求のあとに受信したメッセージは処理せず、visibility timeoutを0に戻してすぐに再配信されるようにします。

受信エラーが続く場合はジッター付きの指数バックオフ(最大30秒)で再試行し、10回連続で失敗した場合は終了コード1で終了し、再起動はスーパーバイザーに委ねます。

## ミドルウェア

ミドルウェアは、ジョブの投入・ポーリング・処理をラップして、トレース伝搬や計測のような横断的な処理を差し込む仕組みです。
使うミドルウェアは`QueueGroup`の`middleware`にリストで渡します。

```python
from sqs_job_worker.contrib.otel import OTelMiddleware

queues = QueueGroup.build(
    boto_client=sqs,
    queues={"default": {"name": "my-app-jobs"}},
    middleware=[OTelMiddleware()],
)
```

ミドルウェアはRackやWSGIと同じように、リストの先頭を最も外側として外から内へ適用されます。
プロデューサーとコンシューマーで同じ`QueueGroup`定義を共有すれば、投入側と処理側の両方に同じミドルウェアが適用されます。

### contribのミドルウェア

コアが依存するのはboto3とstructlogだけで、APMベンダーのSDKやプラットフォームのAPIに触れる連携は`contrib`に分離されています。
APM連携は対応するextraを入れて使います。

- `contrib.newrelic.NewRelicMiddleware` — New Relicの分散トレースを伝搬し、ジョブを`BackgroundTask`として計測して、`trace_id` / `span_id`をログへバインドします
- エージェントがトレースを連結できない場合は、W3C traceparentを自前で合成して、発行側と受信側のログが1つの`trace_id`で突き合わせられる状態を保ちます
- `contrib.otel.OTelMiddleware` — アプリケーションで設定したプロパゲーターでトレースを伝搬し、`messaging.*`属性付きの`CONSUMER`スパンでジョブを計測して、`trace_id` / `span_id`をログへバインドします
- `contrib.sentry.SentryMiddleware` — `sentry-trace` / `baggage`を伝搬し、ハンドラーの例外をSentryへ送信したうえで、リトライの判断はワーカーに委ねます
- `contrib.ecs.EcsDrainMiddleware` — ECSのローリングデプロイ中、同じタスク定義ファミリーのより新しいリビジョンのタスクがRUNNINGになるとポーリングを一時停止し、旧世代のワーカーが新しいジョブを受け取ってしまうのを防ぎます。新世代が失敗して止まった場合は自動的に再開します
  - `EcsDrainMiddleware(boto3.client("ecs"))`のようにECSクライアントを渡します。タスクロールには`ecs:ListTasks`と`ecs:DescribeTasks`の許可が必要です
  - 自身のタスクはECS Task Metadata Endpoint V4から識別され、ECS以外の環境やECS APIのエラー時はフェイルオープンし、通常どおりポーリングを続けます

トレースコンテキストを注入するミドルウェアは1プロセスに1つだけ登録してください。
重複して登録した場合は、ジョブの投入時に`ValueError`として検出されます。

### ミドルウェアを自作する

`JobMiddleware`を継承し、必要なフックだけをオーバーライドします。

- `produce(job, call_next)`は`enqueue()`をラップします。相関フィールドやトレースヘッダーの注入に使います
- `poll(call_next)`はポーリングの1サイクルをラップします。受信の一時停止(ドレイン)やサイクル単位の計測に使います
- `consume(job, call_next)`はハンドラーの実行をラップします。APMトランザクションやジョブ単位の計測に使います

```python
from sqs_job_worker import Job, JobMiddleware

class RequestIdMiddleware(JobMiddleware):
    def produce(self, job: Job, call_next):
        job.correlation_fields.setdefault("request_id", current_request_id())
        return call_next(job)
```

## 相関フィールドとトレース伝搬

- `enqueue(..., correlation_fields={"request_id": "r-1"})`で渡したdictは、`correlation_fields`という1つのメッセージ属性にJSONとしてまとめて格納されます。コンシューマー側では自動的にstructlogのコンテキストへバインドされ、そのジョブが出すすべてのログに付与されます。ただし、`queue`や`message_id`などワーカーが予約するキーは上書きされません
- 相関フィールドは、メッセージの送信権限を持つ任意のプリンシパルが設定できる、信頼されていない観測用メタデータです。認証、認可、テナント選択には使用しないでください
- トレースヘッダー(`traceparent`など)は個別のメッセージ属性として運ばれます。コアはヘッダーの中身を解釈せず、注入と取り出しはcontribのトレーシングミドルウェアが担います
- SQSのメッセージ属性は1メッセージあたり10個までです。これを超える場合は`message_attributes`で明示した属性を優先し、伝搬用の属性を末尾から取り除いたうえで`propagation_attributes_dropped`の警告を出します

## ロギング

structlogで構造化ログを出力します。
出力先やフォーマットの設定はアプリケーション側で行ってください。

ジョブのライフサイクルイベントは`sqs_job_worker.job`ロガーに出力されます。

- `job_enqueued` — 投入時(`queue`、`job_type`、`message_id`)
- `job_started` — 実行開始(`group_id`、`queue_wait_ms`)
- `job_finished` — 終了(`outcome`、`duration_ms`)

`job_finished`の`outcome`は次のいずれかです。

- `success` — 正常終了し、メッセージを削除
- `retry` — ハンドラーが例外を送出。メッセージはvisibility timeoutの経過後に再配信
- `delete` — `NonRetryableError`により、メッセージを削除
- `heartbeat_failed` — visibility timeoutの延長に失敗、または`max_runtime_seconds`を超過。メッセージは削除せず再配信に委ねる
- `delete_failed` — 処理は終えたが削除リクエストが失敗。処理自体の結果は`processing_outcome`に入る

ジョブ処理中のログには`queue` / `message_id` / `receive_count` / `job_type`、相関フィールド、APMミドルウェア使用時は`trace_id` / `span_id`が自動的に付与されます。

## 開発

```console
$ uv sync --all-extras --all-groups
$ uv run pytest
$ uv run ruff check . && uv run ruff format --check .
$ uv run ty check src
```

## ライセンス

[MIT](https://github.com/xmart-corp/sqs-job-worker/blob/main/LICENSE)
