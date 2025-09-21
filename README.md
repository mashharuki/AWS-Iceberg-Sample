# AWS-Apache Iceberg-Sample
Apache Iceberg を学習するためのリポジトリです。

## データレイクとは

構造化データと非構造化データの両方を大規模に保存できるストレージリポジトリのこと。

データレイクは、様々な形式のデータ（構造化、半構造化、非構造化）を元の形式のまま保存できる中央集権的なリポジトリです 。従来のデータウェアハウスとは異なり、データを事前に構造化する必要がありません 。

## Apache Icebergとは

データレイクに置かれたファイルをテーブルのように扱う技術のオープンソースプロジェクトです(テーブルフォーマット規格)。

Netflix社が開発

- 分散ストレージ技術
- 計算エンジン
- テーブル化技術

AWSでもApache Icebergに対応しているサービスがある

- Amazon Athena
- Amazon EMR
- AWS Glue
- Amazon Redshift
- Amazon S3
- Amazon S3 Tables
  - Apache Icebergをネイティブにサポートしたクラウドオブジェクトストア
  - S3テーブルバケットを作成して利用する

## Apache Icebergの構成要素

- クエリエンジン
- Icebergカタログ
  - テーブル情報の管理とトランザクションの一貫性の保証を担う
  - 様々なカタログ実装が存在するが、共通のREST APIでアクセス可能
- ストレージ

## Apache Icebergの登場によるメリット

技術的には、

- データ構造を変更せずにスキーマの柔軟な変更が可能になりビジネス要件に追随しやすい
- トランザクションのサポートによる整合性や信頼性の保証
- ストレージデータに対するスケーラビリティの確保
- **OSSテーブルフォーマットによる計算エンジンとの疎結合化**

## データレイクの基本

多様な生データをデータレイクに蓄積、処理と蓄積を分離する. 

データレイクの中で加工(前処理)し、処理系が使いやすい形に変える

しかしデータレイク上でワークロードがたくさん実行されると...

- 書き込みと読み込みのパフォーマンス
- ソースのスキーマ変更への追従
- GDPR等の特定の個人情報の効率的な削除、更新をしたい要件
- データ削除中のトランザクションの一貫性
- 実験を再現するための過去のスナップショットの参照
- 継続的に更新されるデータに対する一貫性を確保した参照

## 参考文献
- [Apache Iceberg とは何ですか?](https://aws.amazon.com/jp/what-is/apache-iceberg/)
- [【Amazon Athena/Apache Iceberg】AWSの基礎を学ぼう](https://zenn.dev/shigeru_oda/articles/05dbf435200b97e87ee4)
- [Apache Iceberg– なぜ今、Icebergなのか？](https://qiita.com/NateRive/items/e30d48dd162a49a94afe)
- [Amazon AthenaでApache Icebergを使用してみた（その1）](https://www.ctc-g.co.jp/solutions/cloud/column/article/86.html)
- [IceBergの公式サイト](https://iceberg.apache.org/)
- [GitHub - IceBerg](https://github.com/apache/iceberg)
- [Deepwiki - IceBerg](https://deepwiki.com/apache/iceberg)
- [DeepWiki - IceBerg解説](https://deepwiki.com/search/apache-iceberg_94409dc1-14c8-4f97-8ad1-33358e04d953)
- [Serverless Days 2025 Apache Iceberg Workshop](https://catalog.us-east-1.prod.workshops.aws/event/dashboard/en-US)