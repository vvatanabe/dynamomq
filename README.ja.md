# DynamoMQ: Go言語で実装するDynamoDBベースのメッセージキュー

[![Build](https://github.com/vvatanabe/dynamomq/actions/workflows/build.yml/badge.svg)](https://github.com/vvatanabe/dynamomq/actions/workflows/build.yml) [![Go Reference](https://pkg.go.dev/badge/github.com/vvatanabe/dynamomq.svg)](https://pkg.go.dev/github.com/vvatanabe/dynamomq) [![Go Report Card](https://goreportcard.com/badge/github.com/vvatanabe/dynamomq)](https://goreportcard.com/report/github.com/vvatanabe/dynamomq) [![Quality Gate Status](https://sonarcloud.io/api/project_badges/measure?project=vvatanabe_dynamomq&metric=alert_status)](https://sonarcloud.io/summary/new_code?id=vvatanabe_dynamomq) [![Reliability Rating](https://sonarcloud.io/api/project_badges/measure?project=vvatanabe_dynamomq&metric=reliability_rating)](https://sonarcloud.io/summary/new_code?id=vvatanabe_dynamomq) [![Security Rating](https://sonarcloud.io/api/project_badges/measure?project=vvatanabe_dynamomq&metric=security_rating)](https://sonarcloud.io/summary/new_code?id=vvatanabe_dynamomq)  [![Maintainability Rating](https://sonarcloud.io/api/project_badges/measure?project=vvatanabe_dynamomq&metric=sqale_rating)](https://sonarcloud.io/summary/new_code?id=vvatanabe_dynamomq) [![Vulnerabilities](https://sonarcloud.io/api/project_badges/measure?project=vvatanabe_dynamomq&metric=vulnerabilities)](https://sonarcloud.io/summary/new_code?id=vvatanabe_dynamomq) [![Bugs](https://sonarcloud.io/api/project_badges/measure?project=vvatanabe_dynamomq&metric=bugs)](https://sonarcloud.io/summary/new_code?id=vvatanabe_dynamomq) [![Code Smells](https://sonarcloud.io/api/project_badges/measure?project=vvatanabe_dynamomq&metric=code_smells)](https://sonarcloud.io/summary/new_code?id=vvatanabe_dynamomq) [![Duplicated Lines (%)](https://sonarcloud.io/api/project_badges/measure?project=vvatanabe_dynamomq&metric=duplicated_lines_density)](https://sonarcloud.io/summary/new_code?id=vvatanabe_dynamomq) [![Coverage](https://sonarcloud.io/api/project_badges/measure?project=vvatanabe_dynamomq&metric=coverage)](https://sonarcloud.io/summary/new_code?id=vvatanabe_dynamomq)

<p align="center">
  <img height="300" src="https://cacoo.com/diagrams/DjoA2pSKnhCghTYM-192C1.png">
</p>

[@vvatanabe](https://twitter.com/vvvatanabe)です。Go言語を用いてDynamoDBをストレージに活用したメッセージキューイングのライブラリ、DynamoMQを開発しました。このライブラリは、Go言語でのコンシューマーおよびプロデューサーの実装を支援するSDKと、管理ツールとして機能するCLIを提供します。

この記事では、DynamoMQの開発動機、提供する機能、CLIやSDKの使用方法、そしてその設計についてご紹介します。

Note: この記事は、[ヌーラボブログリレー2023 for Tech Advent Calendar 2023 – Adventar](https://adventar.org/calendars/8738) の最終日に掲載されたものです。

## Table of Contents

- [DynamoDBの特性とMQのベースにする利点](#DynamoDBの特性とMQのベースにする利点)
- [既存のAWSキューイングソリューションとの比較](#既存のAWSキューイングソリューションとの比較)
- [DynamoMQがサポートする機能について](#DynamoMQがサポートする機能について)
- [DynamoMQのインストール方法](#DynamoMQのインストール方法)
- [DynamoMQのセットアップ方法](#DynamoMQのセットアップ方法)
- [DynamoMQの認証とクレデンシャル情報](#DynamoMQの認証とクレデンシャル情報)
- [DynamoMQ CLIの使用方法](#DynamoMQ-CLIの使用方法)
- [DynamoMQ SDKの使用方法](#DynamoMQ-SDKの使用方法)
- [DynamoMQの設計について](#DynamoMQの設計について)
- [まとめ](#まとめ)
- [謝辞](#謝辞)
- [著者](#著者)
- [ライセンス](#ライセンス)

## DynamoDBの特性とMQのベースにする利点

メッセージキュー（以下、MQ）を用いたシステムは、アプリケーション間でメッセージを非同期に伝達する目的で広く採用されています。これらのシステムでは、高いスループットと信頼性が特に重視されます。このセクションでは、DynamoDBの特性をMQシステムと組み合わせることの利点を考察します。

### DynamoDBの概要

Amazon DynamoDBは、高性能なNoSQLデータベースサービスで、単純なキーバリューストアから複雑なドキュメント型のストアまで、幅広いデータモデルに対応しています。DynamoDBが持つ主な特徴は以下のとおりです。

- **パフォーマンスとスケーラビリティ**: DynamoDBはミリ秒単位で応答可能であり、必要に応じて自動的に読み書きのキャパシティをスケールアップ・ダウンできます。
- **高可用性と耐久性**: データは自動的に複数の地理的に分散した施設にレプリケーションされるため、高度な可用性と耐久性を実現します。
- **フルマネージドでサーバーレス**: サーバーの管理や設定に関する手間がかからず、運用コストを削減できます。

参考: [What is Amazon DynamoDB?](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Introduction.html)

### MQとDynamoDBの組み合わせの利点

DynamoDBのこれらの特性をMQシステムに応用することで、以下のようなメリットを享受できます。

1. **スケーラビリティ**: 大量のメッセージを効率的に扱う能力は、MQシステムにとって極めて重要です。DynamoDBのオンデマンドモードにより、需要の変動に応じて容易にリソースを調整できます。
2. **コスト効率**: オンデマンドモードを利用することで、リソースのオーバープロビジョニングやアンダープロビジョニングのリスクを低減し、コストを抑えることができます。さらに、使用量を予測できる場合にはプロビジョニングモードを選択することで、コストをさらに削減することも可能です。
3. **信頼性と可用性**: メッセージの喪失はMQにとって致命的です。DynamoDBの高い耐久性と可用性は、メッセージの安全性を保証します。
4. **柔軟性**: DynamoDBに保存されたメッセージの項目を編集することで、キュー内のメッセージの順序を動的に変更したり、属性を更新したり、メッセージの処理をキャンセルするなど、より柔軟な操作が可能になります。

## 既存のAWSキューイングソリューションとの比較

AWSでは、MQに対して、Amazon SQS、Amazon MQ、そしてKafkaに基づくAmazon MSKなど、複数のソリューションを提供しています。これらのサービスとDynamoMQを比較することで、DynamoMQ独自の利点を考察します。

### Amazon SQSとの比較

Amazon SQSはフルマネージドでサーバーレスなMQサービスであり、大規模な分散アプリケーションにおけるメッセージの非同期処理に広く利用されています。

<ins>**スケーラビリティ、コスト効率、信頼性、そして可用性は非常に優れており、筆者の見解としては基本的にAmazon SQSの使用を推奨します。**</ins>

しかし、Amazon SQSは柔軟性に欠ける面もあります。キューに送信されたメッセージは外部から順序を変更したり、メッセージの属性や項目を更新することができません。また、キュー内のメッセージを参照するには、一度受信する必要があります。これらの制約は、特定の問題に対処する際に調査や復旧作業を困難にすることがあります。

### Amazon MQとの比較

Amazon MQは、Apache ActiveMQやRabbitMQをベースにしたメッセージブローカーサービスで、既存のアプリケーションからの移行を主な目的としています。JMS、AMQP、STOMP、MQTT、OpenWire、WebSocketといった多様なプロトコルをサポートしています。

ベースとなるActiveMQやRabbitMQが提供する機能をそのまま利用できるため、Amazon MQは多くの機能を提供しています。しかし、Amazon MQはサーバーレスではないため、セットアップと管理には手間がかかります。

### Amazon MSKとの比較

Amazon MSKは、Apache Kafkaをフルマネージドで提供するサービスで、大量のストリーミングデータ処理に適しています。

MSKはリアルタイムデータストリーミングに特化している一方で、DynamoMQは一般的なメッセージキューイングのニーズに焦点を当てています。また、MSKは高度な設定が可能ですが、それに伴い複雑さとコストが増加する傾向にあります。

### DynamoMQの優位性について

DynamoMQは、DynamoDBのスケーラビリティ、耐久性、そしてサーバーレスという特性を活かし、従来のAWSキューイングソリューションと比較して、メッセージの順序や属性を柔軟に管理できる点、低コストでの運用、そして簡単なセットアップと管理などの優位性を持っています。

## DynamoMQがサポートする機能について

DynamoMQは、MQとして提供すべき主要な機能を備えており、柔軟かつ信頼性の高いシステムの実現を支援します。

### 再配信

メッセージが正常に処理されなかった場合に再配信を行います。これにより、一時的なエラーや処理の遅延に対応し、メッセージが確実に処理される機会を増やします。

### 並列実行

複数のゴルーチンを使用して、メッセージの処理を並行して行います。この機能により、高スループットと効率的なリソース利用が可能になります。

### デッドレターキュー

再配信の最大回数を超えたメッセージをデッドレターキューに移動します。これにより、永続的なエラーを持つメッセージを分離し、後で分析や手動での処理を可能にします。

### グレースフルシャットダウン

コンシューマプロセスのシャットダウン前にメッセージの処理を完了させます。これにより、プロセス終了時に処理中のメッセージが失われることを防ぎます。

### FIFO

FIFO（先入れ先出し）方式でメッセージをキューから取得します。これは、メッセージが送信された順序で処理されることを保証し、順序性が重要なアプリケーションに適しています。

### コンシューマプロセスのスケーリング

複数のコンシューマプロセスを起動し、スケールアウトできます。同じメッセージキューからのメッセージを重複して取得することはありません。これにより、負荷に応じて処理能力を動的に調整できます。

### 可視性タイムアウト

一定期間、すべてのコンシューマによるメッセージの可視性タイムアウトを設定します。これにより、メッセージが処理中に他のコンシューマによって受信されることを防ぎます。

### 遅延キューイング

新しいメッセージの消費者への配信を設定した秒数だけ遅らせる遅延キューイングを提供します。

## DynamoMQのインストール方法

Goのバージョン1.21以上が必要です。

### DynamoMQ CLIのインストール

以下のgo installコマンドを使用して、このパッケージをCLIとしてインストールできます:
```
$ go install github.com/vvatanabe/dynamomq/cmd/dynamomq@latest
```

### DynamoMQ SDKのインストール

以下のgo getコマンドを使用して、このパッケージをSDKとしてインストールできます:
```
$ go get -u github.com/vvatanabe/dynamomq@latest
```

## DynamoMQのセットアップ方法

必要なIAMポリシーとテーブルの作成方法について説明します。

### 必要なIAMポリシーについて

必要なIAMポリシーに関する詳細は、[dynamomq-iam-policy.json](./dynamomq-iam-policy.json) または [dynamomq-iam-policy.tf](./dynamomq-iam-policy.tf) をご覧ください。

### AWS CLIを利用したテーブルの作成方法

以下のコマンドを使用してAWS CLIでテーブルを作成します:
```sh
aws dynamodb create-table --cli-input-json file://dynamomq-table.json 
```
[dynamomq-table.json](./dynamomq-table.json) をご覧ください。

### Terraformを利用したテーブルの作成方法

詳細は、[dynamomq-table.tf](./dynamomq-table.tf) をご覧ください。

## DynamoMQの認証とクレデンシャル情報

DynamoMQのCLIとSDKでは、外部の設定ソースから取得されたクレデンシャル情報を用います。設定のためのデフォルトソースは以下の通りです:

### 環境変数

- `AWS_REGION` - AWSリージョンを指定します。
- `AWS_PROFILE` - 使用するAWSプロファイルを指定します。
- `AWS_ACCESS_KEY_ID` - AWSアクセスキーを指定します。
- `AWS_SECRET_ACCESS_KEY` - AWSシークレットキーを指定します。
- `AWS_SESSION_TOKEN` - 一時的なクレデンシャルのセッショントークンを指定します。

## DynamoMQ CLIの使用方法

DynamoMQ CLIは、DynamoDBベースのMQとやりとりするために一連のコマンドを提供しています。以下に、`dynamomq`で利用可能なコマンドとグローバルフラグを紹介します。

### 利用可能なコマンド一覧

- `completion`: 指定されたシェル用の自動補完スクリプトを生成し、コマンドの利用を容易にします。
- `delete`: IDを使ってキューからメッセージを削除します。
- `dlq`: デッドレターキュー（DLQ）の統計を取得し、メッセージ処理の失敗に関する情報を提供します。
- `enqueue-test`: IDがA-101、A-202、A-303、A-404のテストメッセージをDynamoDBテーブルに送信します。既存の同じIDのメッセージは上書きされます。
- `fail`: メッセージ処理の失敗をシミュレートし、メッセージをキューに戻します。
- `get`: IDを使ってDynamoDBテーブルから特定のメッセージを取得します。
- `help`: 各コマンドに関するヘルプ情報を表示します。
- `invalid`: スタンダードキューからDLQにメッセージを移動し、手動での確認と修正を行います。
- `ls`: キュー内の全てのメッセージのIDをリストを最大10要素を表示します。
- `purge`: DynamoMQテーブルから全てのメッセージを削除し、キューをクリアします。
- `qstat`: キューの統計情報を取得し、その現在の状態についての概要を提供します。
- `receive`: キューからメッセージを受信します。
- `redrive`: DLQからスタンダードキューへメッセージを戻し、再処理します。
- `reset`: メッセージのシステム情報をリセットします。

### グローバルフラグ

- `--endpoint-url`: 特定のエンドポイントURLを指定してデフォルトのURLを上書きします。
- `-h`, `--help`: `dynamomq`に関するヘルプ情報を表示します。
- `--queueing-index-name`: 使用するキューインデックスの名前を指定します（デフォルトは`"dynamo-mq-index-queue_type-sent_at"`）。
- `--table-name`: アイテムを格納するDynamoDBテーブルの名前を定義します（デフォルトは`"dynamo-mq-table"`）。

特定のコマンドに関する詳細情報を取得するには、`dynamomq [command] --help`を使用してください。

### インタラクティブモード

DynamoMQ CLIはインタラクティブモードをサポートしています。インタラクティブモードに入るには、サブコマンドを指定せずに`dynamomq`コマンドを実行します。

#### インタラクティブモードで利用可能なコマンド

インタラクティブモードでは、以下のコマンドを提供します:

- `qstat`: キューの統計情報を取得します。
- `dlq`: デッドレターキュー（DLQ）の統計情報を取得します。
- `enqueue-test`または`et`: IDがA-101、A-202、A-303、A-404のテストメッセージをDynamoDBテーブルに送信します。既存のメッセージが存在する場合は上書きされます。
- `purge`: DynamoMQテーブルから全てのメッセージを削除します。
- `ls`: 最大10要素までの全メッセージIDをリスト表示します。
- `receive`: キューからメッセージを受信します。
- `id <id>`: IDを指定して対象のメッセージに対して様々な操作を実行できるようにします:
  - `sys`: メッセージのシステム情報をJSON形式で表示します。
  - `data`: 現在のメッセージの`data`部分ををJSONで出力します。
  - `info`: メッセージに関する全情報をJSON形式で出力します。
  - `reset`: メッセージのシステム情報をリセットします。
  - `redrive`: DLQからスタンダードキューへメッセージを戻します。
  - `delete`: IDを使ってメッセージを削除します。
  - `fail`: キューに戻されるメッセージの処理失敗をシミュレートします。その後、メッセージは再受信できるようなります。
  - `invalid`: スタンダードキューからDLQへメッセージを移動します。

## DynamoMQ SDKの使用方法

DynamoMQは、Go言語用のSDKを提供し、コンシューマーとプロデューサーの実装をサポートします。以下に、DynamoMQライブラリを使用するための簡単な例を示します。詳細については[GoDoc](https://pkg.go.dev/github.com/vvatanabe/dynamomq)を参照してください。

### DynamoMQクライアントの設定

DynamoMQの利用を始めるには、まずAWS SDK for Go v2とDynamoMQから必要なパッケージをインポートします。これらは、AWSサービスとDynamoMQの機能にアクセスするために必要です。

```go
import (
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/vvatanabe/dynamomq"
)
```

次のコードブロックでDynamoMQクライアントを初期化します。AWSの設定を読み込み、その設定を使って新しいDynamoMQクライアントを作成します。ジェネリクスで指定している'ExampleData'を必要に応じて独自のデータ構造に置き換えてください。

```go
ctx := context.Background()
cfg, err := config.LoadDefaultConfig(ctx)
if err != nil {
  panic("failed to load aws config")
}
client, err := dynamomq.NewFromConfig[ExampleData](cfg)
if err != nil {
  panic("AWS session could not be established!")
}
```

DynamoMQで使用するデータ構造を定義します。'ExampleData'はDynamoDBのデータを表すために使用される構造体です。

```go
type ExampleData struct {
	Data1 string `dynamodbav:"data_1"`
	Data2 string `dynamodbav:"data_2"`
	Data3 string `dynamodbav:"data_3"`
}
```

### DynamoMQプロデューサーの作成

次のスニペットでは、'ExampleData'型のDynamoMQプロデューサーを作成し、事前に定義されたデータを持つメッセージをキューに送信します。

```go
producer := dynamomq.NewProducer[ExampleData](client)
_, err = producer.Produce(ctx, &dynamomq.ProduceInput[ExampleData]{
  Data: ExampleData{
    Data1: "foo",
    Data2: "bar",
    Data3: "baz",
  },
})
if err != nil {
  panic("failed to produce message")
}
```

### DynamoMQコンシューマーの作成

メッセージを消費するために、'ExampleData'のDynamoMQコンシューマーをインスタンス化し、新しいゴルーチンで起動します。コンシューマーは、中断シグナルを受け取るまでメッセージを処理し続けます。以下の例には、コンシューマーのグレースフルシャットダウンのロジックが含まれています。

```go
consumer := dynamomq.NewConsumer[ExampleData](client, &Counter[ExampleData]{})
go func() {
  err = consumer.StartConsuming()
  if err != nil {
    fmt.Println(err)
  }
}()

done := make(chan os.Signal, 1)
signal.Notify(done, os.Interrupt, syscall.SIGTERM)

<-done

ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
defer cancel()

if err := consumer.Shutdown(ctx); err != nil {
  fmt.Println("failed to consumer shutdown:", err)
}
```

以下に、消費されたメッセージを処理するための'Counter'型を定義します。メッセージが処理されるたびにカウンターが増え、メッセージの詳細が表示されます。

```go
type Counter[T any] struct {
	Value int
}

func (c *Counter[T]) Process(msg *dynamomq.Message[T]) error {
	c.Value++
	fmt.Printf("value: %d, message: %v\n", c.Value, msg)
	return nil
}
```

## DynamoMQの設計について

DynamoMQの内部的な設計について説明します。

### メッセージの属性とテーブル定義

以下はDynamoMQがメッセージキューを実現するためのテーブル定義を示した図です。

| Key   | Attributes         | Type   | Example Value                       |
|-------|--------------------|--------|-------------------------------------|
| PK    | id                 | string | A-101                               |
|       | data               | any    | any                                 |
|       | receive_count      | number | 1                                   |
| GSIPK | queue_type         | string | STANDARD or DLQ                     |
|       | version            | number | 1                                   |
|       | created_at         | string | 2006-01-02T15:04:05.999999999Z07:00 |
|       | updated_at         | string | 2006-01-02T15:04:05.999999999Z07:00 |
| GSISK | sent_at            | string | 2006-01-02T15:04:05.999999999Z07:00 |
|       | received_at        | string | 2006-01-02T15:04:05.999999999Z07:00 |
|       | invisible_until_at | string | 2006-01-02T15:04:05.999999999Z07:00 |

このテーブル定義に基づき、各属性とグローバルセカンダリインデックス（以下、GSI）について解説します。

#### id（パーティションキー）

メッセージの一意な識別子で、DynamoDBのパーティションキーとして機能します。これによりDynamoDB内のデータの分散と効率的なアクセスが可能になります。DynamoMQのパブリッシャーを使用すると、デフォルトでUUIDが自動生成されます。ユーザーは独自のIDを指定することも可能です。

#### data  

メッセージに含まれるデータの内容です。DynamoDBがサポートする任意の形式で保存できます。

参考:
- [DynamoDB Data Types](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/DynamoDBMapper.DataTypes.html)
- [DynamoDB Item sizes and formats](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/CapacityUnitCalculations.html)

#### receive_count

メッセージが受信された回数を表します。この数値は再試行やデッドレターキューへの移動を管理する際に使用されます。

#### queue_type（GSIのプライマリーキー）

メッセージが保存されているキューのタイプを示します。スタンダードキュー（STANDARD）かデッドレターキュー（DLQ）かを区別します。

#### version

メッセージのバージョン番号です。メッセージが更新されるたびにこの数値は増加し、楽観的排他制御を実現します。

#### created_at  

メッセージが作成された日時です。ISO 8601形式で記録されます。

#### updated_at  

メッセージが最後に更新された日時です。ISO 8601形式で記録されます。

#### sent_at (Sort Key for GSI)

メッセージがキューに送信された日時です。ISO 8601形式で記録されます。

#### received_at

メッセージが受信された日時です。ISO 8601形式で記録されます。

#### invisible_until_at

メッセージがキュー内で次に可視化される日時です。この時間が過ぎると、メッセージは再度受信可能になります。

#### グローバルセカンダリインデックス（GSI）

キューに追加された順序でメッセージを受信するために、`queue_type`をパーティションキーとし、`sent_at`をソートキーとするGSIが設定されています。

### メッセージの状態遷移

以下の状態遷移図は、DynamoMQにおけるメッセージのライフサイクルと、それが取りうる状態の遷移を示しています。図には、スタンダードキュー（`queue_type=STANDARD`）とデッドレターキュー（`queue_type=DLQ`）でのメッセージの処理方法が示されています。

![State Machine](https://cacoo.com/diagrams/DjoA2pSKnhCghTYM-9383C.png) 

また、以下の図ではメッセージの属性が状態遷移に伴いどのように変化するかを示しています。赤色で示された属性は状態遷移によって更新されます。

![Data Transition](https://cacoo.com/diagrams/DjoA2pSKnhCghTYM-DCE15.png)

これらの図に示されたプロセスをステップバイステップで説明します。

#### スタンダードキューの状態遷移の説明

1. **メッセージの送信**
   - プロデューサーは、`SendMessage()`関数を使用してスタンダードキューにメッセージを送信します。
   - 送信されたメッセージは「READY」状態になり、これはメッセージが受信可能な状態を意味します。
   - このメッセージには一意のIDが割り当てられ、`data`フィールドには処理されるべきデータが含まれます。
   - メッセージの`queue_type`属性は`STANDARD`に設定され、バージョンは初期状態で `1` になります。
   - `created_at`、`updated_at`、`sent_at`のタイムスタンプが記録されます。

2. **メッセージの受信**
   - コンシューマーは、`ReceiveMessage()`関数を利用してメッセージを受信し、処理を開始します。
   - メッセージは「PROCESSING」状態に移行し、この期間中は他のコンシューマーからは見えなくなり、受信されなくなります。
   - メッセージの`receive_count`と`version`はそれぞれ受信の度にインクリメントされます。
   - `updated_at`、`received_at`のタイムスタンプが更新され、`invisible_until_at`が新しい日時で設定されます。

3. **処理の成功**
   - 処理が成功した場合、コンシューマーは`DeleteMessage()`関数を使用してメッセージをキューから削除します。

4. **処理の失敗**
   - 処理に失敗した場合、特にリトライが必要な場合は、`ChangeMessageVisibility()`を使用してメッセージの`invisible_until_at`属性を更新し、後で再処理できるようにします。
   - `invisible_until_at`の設定された日時が過ぎれば、メッセージは再び「READY」状態に戻り、再度受信が可能になります。

#### デッドレターキューの状態遷移の説明

1. **DLQへの移動**
   - メッセージの処理が最大再配信回数を超えて失敗すると、`MoveMessageToDLQ()`関数によってメッセージはデッドレターキュー（DLQ）に移動されます。
   - この時、メッセージの`queue_type`は`DLQ`に変更され、`receive_count`は`0`にリセットされます。
   - DLQに移動されたメッセージは、再び「READY」状態に戻ります。

2. **メッセージの受信**
   - DLQ内で、メッセージは再び`ReceiveMessage()`によって受信され、`PROCESSING`状態に遷移します。
   - この期間中、メッセージは他のコンシューマーからは見えなくなります。

3. **処理の成功**
   - メッセージが正常に処理されると、`DeleteMessage()`によってDLQから削除されます。

4. **スタンダードキューへの移動**
   - `RedriveMessage()`を使用してメッセージを元のスタンダードキューに戻すことも可能です。
   - この操作により、`queue_type`は再び`STANDARD`に設定され、メッセージは「READY」状態に戻ります。

## まとめ

DynamoMQは、DynamoDBの特性を生かし、高いスケーラビリティ、信頼性、およびコスト効率を実現するメッセージキューのライブラリです。特に、メッセージの順序変更や属性を動的に編集できる点は、アプリケーションの要求を柔軟に対応することを可能にします。

既存のソリューションと比較して、DynamoMQは開発者にとって管理が容易でありながら、Amazon SQSのようなフルマネージドサービスの信頼性を提供します。また、複数のゴルーチンによる並列実行処理や、デッドレターキュー、FIFO順序の保証など、メッセージキューが提供すべき主要な機能を網羅しています。

## 謝辞

DynamoMQの開発にあたり参考とさせていただいたAWSの公式ブログ「[Implementing priority queueing with Amazon DynamoDB](https://aws.amazon.com/blogs/database/implementing-priority-queueing-with-amazon-dynamodb/)」に深く感謝申し上げます。このブログでは、Amazon DynamoDBを使用した優先度付きキューイングの実装方法が詳細に解説されており、その知識がDynamoMQの構築に非常に役立ちました。

## 著者

* **[vvatanabe](https://github.com/vvatanabe/)** - *主要な貢献者*

## ライセンス

このプロジェクトはMITライセンスです。詳細なライセンス情報については、リポジトリに含まれる[LICENSE](LICENSE)ファイルを参照してください。
