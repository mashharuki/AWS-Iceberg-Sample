# IceBergに関するワークショップメモ

## ハンズオンの記録

- まずはマネジメントコンソールにログインしてEMRのセットアップを行う必要あり
- Studioを作成する
  - IAMロールの詳細
    - 信頼ポリシー

      ```json
      {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Principal": {
                    "Service": "elasticmapreduce.amazonaws.com"
                },
                "Action": "sts:AssumeRole",
                "Condition": {
                    "StringEquals": {
                        "aws:SourceAccount": "860010260849"
                    },
                    "ArnLike": {
                        "aws:SourceArn": "arn:aws:elasticmapreduce:us-east-1:860010260849:*"
                    }
                }
            }
        ]
      }
      ```

    - IAMポリシー

      ```json
      {
          "Version": "2012-10-17",
          "Statement": [
              {
                  "Sid": "ObjectActions",
                  "Effect": "Allow",
                  "Action": [
                      "s3:PutObject",
                      "s3:GetObject",
                      "s3:DeleteObject"
                  ],
                  "Resource": [
                      "arn:aws:s3:::aws-emr-studio-860010260849-us-east-1/*"
                  ]
              },
              {
                  "Sid": "BucketActions",
                  "Effect": "Allow",
                  "Action": [
                      "s3:ListBucket",
                      "s3:GetEncryptionConfiguration"
                  ],
                  "Resource": [
                      "arn:aws:s3:::aws-emr-studio-860010260849-us-east-1"
                  ]
              }
          ]
      }
      ```

# Amazon AthenaとIceBergの組み合わせ例

まずAthenaのセットアップ

- Trino SQL を使用してデータをクエリする を選択してクエリエディタを起動する
- クエリ対象のS3をs3://otfs-workshop-data-860010260849/athena_res/に指定する
- 以下のクエリでS3 Tablesを作成する

  ```sql
  create database athena_iceberg_db;
  ```

- 以下のクエリでIceberg用のテーブルを作成する

  'table_type'が'iceberg'になっているところがミソ！

  ```sql
  CREATE TABLE athena_iceberg_db.customer_iceberg (
      c_customer_sk INT COMMENT 'unique id', 
      c_customer_id STRING, 
      c_first_name STRING, 
      c_last_name STRING, 
      c_email_address STRING)
  LOCATION 's3://otfs-workshop-data-860010260849/datasets/athena_iceberg/customer_iceberg'
  TBLPROPERTIES (
    'table_type'='iceberg',
    'format'='PARQUET',
    'write_compression'='zstd'
  );
  ```

- 作成済みのテーブルを確認

  ```sql
  SHOW TABLES IN athena_iceberg_db;
  ```

  ```bash
  customer_iceberg
  ```

- スキーマの確認

  ```sql
  DESCRIBE customer_iceberg;
  ```

  ```bash
  # Table schema:		
  # col_name	data_type	comment
  c_customer_sk	int	unique id
  c_customer_id	string	
  c_first_name	string	
  c_last_name	string	
  c_email_address	string	
      
  # Partition spec:		
  # field_name	field_transform	column_name
  ```

- テーブルのメタデータを情報を取得する

  ```sql
  SELECT * FROM "athena_iceberg_db"."customer_iceberg$files"
  ```

- テーブルのマニュフェストを取得する

  ```sql
  SELECT * FROM "athena_iceberg_db"."customer_iceberg$manifests"
  ```

- テーブルのスナップショットを取得する

  ```sql
  SELECT * FROM "athena_iceberg_db"."customer_iceberg$snapshots"
  ```

- 検証用のデータをテーブルに挿入する

  `tpcds.prepared_customer`のデータを挿入する

  ```sql
  INSERT INTO athena_iceberg_db.customer_iceberg
  SELECT * FROM tpcds.prepared_customer 
  ```

- データを取得する

  ```sql
  select * from athena_iceberg_db.customer_iceberg limit 10;
  ```

  ```bash
  #	c_customer_sk	c_customer_id	c_first_name	c_last_name	c_email_address
  1	1116617	AAAAAAAAJMJABBAA	Evelyn	Hendricks	Evelyn.Hendricks@Nu.edu
  2	1116618	AAAAAAAAKMJABBAA	Nathan	Meyer	Nathan.Meyer@4hUOoh.com
  3	1116619	AAAAAAAALMJABBAA	Charles	Bass	Charles.Bass@FjNaxQpcDdCNCRnd.edu
  4	1116624	AAAAAAAAANJABBAA	Martin	Schroeder	Martin.Schroeder@Of7GkhBeD.edu
  5	1116625	AAAAAAAABNJABBAA	Brad	Ramsey	Brad.Ramsey@D39A.com
  6	1116626	AAAAAAAACNJABBAA			
  7	1116620	AAAAAAAAMMJABBAA	Kent		
  8	1116621	AAAAAAAANMJABBAA	Eduardo	Hill	Eduardo.Hill@XRMViNUXk.org
  9	1116622	AAAAAAAAOMJABBAA	James	Nieves	James.Nieves@VN1L08stjDhh.com
  10	1116623	AAAAAAAAPMJABBAA	Inez	Lipscomb	Inez.Lipscomb@fPigt.com
  ```

  ```sql
  select count(*) from athena_iceberg_db.customer_iceberg;
  ```

  ```bash
  2000000
  ```

- 履歴を取得する

  ```sql
  SELECT * FROM "athena_iceberg_db"."customer_iceberg$history"
  ```

  ```bash
  #	made_current_at	snapshot_id	parent_id	is_current_ancestor
  1	2025-09-21 07:26:00.690 UTC	3367024045542236357		true
  ```

- 次にレコードを更新してみる

  ```sql
  select * from athena_iceberg_db.customer_iceberg
  WHERE c_customer_sk = 15
  ```

  ```bash
  #	c_customer_sk	c_customer_id	c_first_name	c_last_name	c_email_address
  1	15	AAAAAAAAPAAAAAAA	Tonya		
  ```

  データを更新する

  ```sql
  UPDATE athena_iceberg_db.customer_iceberg
  SET c_last_name = 'John', c_email_address = 'johnTonya@abx.com' WHERE c_customer_sk = 15
  ```

  再びデータを取得してみると結果が変わっているはず

  ```bash
  #	c_customer_sk	c_customer_id	c_first_name	c_last_name	c_email_address
  1	15	AAAAAAAAPAAAAAAA	Tonya	John	johnTonya@abx.com
  ```

  次にS3Tableからステートメントリストを取得する

  ```sql
  SELECT * FROM "athena_iceberg_db"."customer_iceberg$files"
  ```

  ```bash
  #	content	file_path	file_format	record_count	file_size_in_bytes	column_sizes	value_counts	null_value_counts	nan_value_counts	lower_bounds	upper_bounds	key_metadata	split_offsets	equality_ids
  1	0	s3://otfs-workshop-data-860010260849/datasets/athena_iceberg/customer_iceberg/data/FCOxig/20250921_072553_00039_vj8a8-692f4aaf-4737-417a-9fa4-ff0fdaf3acd8.parquet	PARQUET	2000000	43991498	{1=5770471, 2=1341044, 3=3023389, 4=3147188, 5=30708714}	{1=2000000, 2=2000000, 3=2000000, 4=2000000, 5=2000000}	{1=0, 2=0, 3=0, 4=0, 5=0}	{}	{1=1, 2=AAAAAAAAAAAAABAA, 3=, 4=, 5=}	{1=2000000, 2=AAAAAAAAPPPPPAAA, 3=Zulma, 4=Zuniga, 5=Zulma.Wright@Aqp}			
  2	0	s3://otfs-workshop-data-860010260849/datasets/athena_iceberg/customer_iceberg/data/rDoO_A/20250921_073357_00063_4rihd-cd2336ee-e4c7-4706-b490-1c666565ea7f.parquet	PARQUET	1	957	{1=40, 2=56, 3=45, 4=44, 5=57}	{1=1, 2=1, 3=1, 4=1, 5=1}	{1=0, 2=0, 3=0, 4=0, 5=0}	{}	{1=15, 2=AAAAAAAAPAAAAAAA, 3=Tonya, 4=John, 5=johnTonya@abx.co}	{1=15, 2=AAAAAAAAPAAAAAAA, 3=Tonya, 4=John, 5=johnTonya@abx.cp}			
  ```

  次にS3 Tablesからデータを削除してみる

  ```sql
  delete from athena_iceberg_db.customer_iceberg
  WHERE c_customer_sk = 15
  ```

  データをselectしてみて削除されていればOK!

  ```sql
  SELECT * FROM athena_iceberg_db.customer_iceberg WHERE c_customer_sk = 15
  ```

- Time Travelを有効化する

  ```sql
  SELECT * FROM "athena_iceberg_db"."customer_iceberg$history" 
  order by made_current_at;
  ```

  ```bash
  #	made_current_at	snapshot_id	parent_id	is_current_ancestor
  1	2025-09-21 07:26:00.690 UTC	3367024045542236357		true
  2	2025-09-21 07:33:59.161 UTC	8901800667988350258	3367024045542236357	true
  3	2025-09-21 07:37:03.567 UTC	5430518374707242081	8901800667988350258	true
  ```

  - Row1:  
    corresponds to the initial insert operation we performed to populate the table. The snapshot_id column shows the first created snapshot.
  - Row2:  
    corresponds to the update operation we performed. The snapshot_id column shows the second created snapshot.
  - Row3:  
    corresponds to the delete operation we performed. The snapshot_id column shows the third (latest) created snapshot.

- スナップショットIDを指定してデータを取得する

  ```sql
  select * from athena_iceberg_db.customer_iceberg 
  FOR VERSION AS OF  8901800667988350258
  WHERE c_customer_sk = 15
  ```

  ちゃんと削除する前のデータが取得できればOK!

  ```bash
  #	c_customer_sk	c_customer_id	c_first_name	c_last_name	c_email_address
  1	15	AAAAAAAAPAAAAAAA	Tonya	John	johnTonya@abx.com
  ```

  それかタイムスタンプを指定する形でも取得が可能

  ```sql
  select * from athena_iceberg_db.customer_iceberg
  FOR TIMESTAMP AS OF TIMESTAMP '2025-09-21 07:33:59.161 UTC'
  WHERE c_customer_sk = 15
  ```

  やはりこちらでも取得が可能

  ```bash
  #	c_customer_sk	c_customer_id	c_first_name	c_last_name	c_email_address
  1	15	AAAAAAAAPAAAAAAA	Tonya	John	johnTonya@abx.com
  ```

- スキーマの進化

  Icebergのスキーマ更新は、メタデータのみの変更です。  
  スキーマを更新しても、データファイルは一切変更されません。  
  Icebergフォーマットは、以下のスキーマ進化の変更をサポートしています。

  - 追加 - テーブルまたはネストされた構造体（struct）に新しいカラムを追加します。
  - 削除 - テーブルまたはネストされた構造体から既存のカラムを削除します。
  - 名前変更 - 既存のカラム、またはネストされた構造体のフィールドの名前を変更します。
  - 並び替え - カラムの順序を変更します。
  - 型の昇格 - カラム、構造体フィールド、マップキー、マップ値、またはリスト要素の型をより広い型に変換します。現在、Icebergテーブルでは以下のケースがサポートされています。
    - integerからbig integerへの変換
    - floatからdoubleへの変換
    - decimal型の精度を上げる


  ```sql
  SELECT * FROM "athena_iceberg_db"."customer_iceberg$files"
  ```

  ```bash
  #	content	file_path	file_format	record_count	file_size_in_bytes	column_sizes	value_counts	null_value_counts	nan_value_counts	lower_bounds	upper_bounds	key_metadata	split_offsets	equality_ids
  1	0	s3://otfs-workshop-data-860010260849/datasets/athena_iceberg/customer_iceberg/data/FCOxig/20250921_072553_00039_vj8a8-692f4aaf-4737-417a-9fa4-ff0fdaf3acd8.parquet	PARQUET	2000000	43991498	{1=5770471, 2=1341044, 3=3023389, 4=3147188, 5=30708714}	{1=2000000, 2=2000000, 3=2000000, 4=2000000, 5=2000000}	{1=0, 2=0, 3=0, 4=0, 5=0}	{}	{1=1, 2=AAAAAAAAAAAAABAA, 3=, 4=, 5=}	{1=2000000, 2=AAAAAAAAPPPPPAAA, 3=Zulma, 4=Zuniga, 5=Zulma.Wright@Aqp}			
  2	0	s3://otfs-workshop-data-860010260849/datasets/athena_iceberg/customer_iceberg/data/rDoO_A/20250921_073357_00063_4rihd-cd2336ee-e4c7-4706-b490-1c666565ea7f.parquet	PARQUET	1	957	{1=40, 2=56, 3=45, 4=44, 5=57}	{1=1, 2=1, 3=1, 4=1, 5=1}	{1=0, 2=0, 3=0, 4=0, 5=0}	{}	{1=15, 2=AAAAAAAAPAAAAAAA, 3=Tonya, 4=John, 5=johnTonya@abx.co}	{1=15, 2=AAAAAAAAPAAAAAAA, 3=Tonya, 4=John, 5=johnTonya@abx.cp}			
  ```

  カラムの型を変更

  ```sql
  ALTER TABLE athena_iceberg_db.customer_iceberg change column c_email_address email STRING
  ```

  再度以下を実行して結果を取得する

  ```sql
  SELECT * FROM "athena_iceberg_db"."customer_iceberg$files"
  ```

  ```bash
  #	content	file_path	file_format	record_count	file_size_in_bytes	column_sizes	value_counts	null_value_counts	nan_value_counts	lower_bounds	upper_bounds	key_metadata	split_offsets	equality_ids
  1	0	s3://otfs-workshop-data-860010260849/datasets/athena_iceberg/customer_iceberg/data/rDoO_A/20250921_073357_00063_4rihd-cd2336ee-e4c7-4706-b490-1c666565ea7f.parquet	PARQUET	1	957	{1=40, 2=56, 3=45, 4=44, 5=57}	{1=1, 2=1, 3=1, 4=1, 5=1}	{1=0, 2=0, 3=0, 4=0, 5=0}	{}	{1=15, 2=AAAAAAAAPAAAAAAA, 3=Tonya, 4=John, 5=johnTonya@abx.co}	{1=15, 2=AAAAAAAAPAAAAAAA, 3=Tonya, 4=John, 5=johnTonya@abx.cp}			
  2	0	s3://otfs-workshop-data-860010260849/datasets/athena_iceberg/customer_iceberg/data/FCOxig/20250921_072553_00039_vj8a8-692f4aaf-4737-417a-9fa4-ff0fdaf3acd8.parquet	PARQUET	2000000	43991498	{1=5770471, 2=1341044, 3=3023389, 4=3147188, 5=30708714}	{1=2000000, 2=2000000, 3=2000000, 4=2000000, 5=2000000}	{1=0, 2=0, 3=0, 4=0, 5=0}	{}	{1=1, 2=AAAAAAAAAAAAABAA, 3=, 4=, 5=}	{1=2000000, 2=AAAAAAAAPPPPPAAA, 3=Zulma, 4=Zuniga, 5=Zulma.Wright@Aqp}			
  ```

  カラムを追加

  ```sql
  ALTER TABLE athena_iceberg_db.customer_iceberg ADD COLUMNS (c_birth_date int)
  ```

  ```sql
  SELECT * FROM "athena_iceberg_db"."customer_iceberg$files"
  ```

  変更されたかどうかは以下で確認

  ```sql
  DESCRIBE customer_iceberg;
  ```

  型情報はカラムが新たに加わっているはず

  ```bash
  # Table schema:		
  # col_name	data_type	comment
  c_customer_sk	int	unique id
  c_customer_id	string	
  c_first_name	string	
  c_last_name	string	
  email	string	
  c_birth_date	int	
      
  # Partition spec:		
  # field_name	field_transform	column_name
  ```

  以下で取得

  ```sql
  SELECT *
  FROM athena_iceberg_db.customer_iceberg
  LIMIT 10
  ```

  するとスキーマが更新されたバージョンでデータが取得される

  ```bash
  #	c_customer_sk	c_customer_id	c_first_name	c_last_name	email	c_birth_date
  1	1116617	AAAAAAAAJMJABBAA	Evelyn	Hendricks	Evelyn.Hendricks@Nu.edu	
  2	1116618	AAAAAAAAKMJABBAA	Nathan	Meyer	Nathan.Meyer@4hUOoh.com	
  3	1116619	AAAAAAAALMJABBAA	Charles	Bass	Charles.Bass@FjNaxQpcDdCNCRnd.edu	
  4	1116620	AAAAAAAAMMJABBAA	Kent			
  5	1116621	AAAAAAAANMJABBAA	Eduardo	Hill	Eduardo.Hill@XRMViNUXk.org	
  6	1116622	AAAAAAAAOMJABBAA	James	Nieves	James.Nieves@VN1L08stjDhh.com	
  7	1116623	AAAAAAAAPMJABBAA	Inez	Lipscomb	Inez.Lipscomb@fPigt.com	
  8	1116624	AAAAAAAAANJABBAA	Martin	Schroeder	Martin.Schroeder@Of7GkhBeD.edu	
  9	1116625	AAAAAAAABNJABBAA	Brad	Ramsey	Brad.Ramsey@D39A.com	
  10	1116626	AAAAAAAACNJABBAA				
  ```

## 応用編！

S3 Tablesを新たに作成する

```sql
CREATE TABLE athena_iceberg_db.web_sales_iceberg (
    ws_order_number INT,
    ws_item_sk INT,
    ws_quantity INT,
    ws_sales_price DOUBLE,
    ws_warehouse_sk INT,
    ws_sales_time TIMESTAMP)
PARTITIONED BY (year(ws_sales_time))
LOCATION 's3://otfs-workshop-data-860010260849/datasets/athena_iceberg/web_sales_iceberg'
TBLPROPERTIES (
  'table_type'='iceberg',
  'format'='PARQUET',
  'write_compression'='ZSTD'
);
```

データを挿入する

```sql
INSERT INTO athena_iceberg_db.web_sales_iceberg
SELECT * FROM tpcds.prepared_web_sales where year(ws_sales_time) < 2001; 
```

データが入っていることなどを確認

```sql
SELECT *
FROM athena_iceberg_db.web_sales_iceberg
LIMIT 10;
```

```bash
#	ws_order_number	ws_item_sk	ws_quantity	ws_sales_price	ws_warehouse_sk	ws_sales_time
1	691592	81092	83	81.61	15	2000-11-03 14:58:08.000000
2	691592	175028	38	19.73	7	2000-11-03 14:58:08.000000
3	691592	108205	67	27.66	3	2000-11-03 14:58:08.000000
4	691592	51581	32	19.2	5	2000-11-03 14:58:08.000000
5	691592	144587	27	82.49	10	2000-11-03 14:58:08.000000
6	691592	93140	28	1.21	11	2000-11-03 14:58:08.000000
7	691592	110021	31	139.96	14	2000-11-03 14:58:08.000000
8	691592	59690	93	56.84	4	2000-11-03 14:58:08.000000
9	691592	40805	26	45.07	1	2000-11-03 14:58:08.000000
10	691592	64631	34	59.19	11	2000-11-03 14:58:08.000000
```

```sql
SELECT * FROM "athena_iceberg_db"."web_sales_iceberg$files"
```

```bash
#	content	file_path	file_format	record_count	file_size_in_bytes	column_sizes	value_counts	null_value_counts	nan_value_counts	lower_bounds	upper_bounds	key_metadata	split_offsets	equality_ids
1	0	s3://otfs-workshop-data-860010260849/datasets/athena_iceberg/web_sales_iceberg/data/UHn_bw/ws_sales_time_year=2000/20250921_075743_00015_gcdxk-1b26154d-4f84-4186-b850-8179e0137246.parquet	PARQUET	14457568	89032038	{1=3261796, 2=30044555, 3=12684325, 4=27231071, 5=7260491, 6=8548903}	{1=14457568, 2=14457568, 3=14457568, 4=14457568, 5=14457568, 6=14457568}	{1=0, 2=0, 3=424, 4=416, 5=0, 6=0}	{}	{1=3, 2=1, 3=1, 4=-0.0, 5=1, 6=2000-01-01T00:01:03}	{1=6000000, 2=203999, 3=100, 4=299.7, 5=15, 6=2000-12-31T23:59:20}			
2	0	s3://otfs-workshop-data-860010260849/datasets/athena_iceberg/web_sales_iceberg/data/tGtcdw/ws_sales_time_year=1998/20250921_075743_00015_gcdxk-d30624a0-3f45-4eaa-ba23-d96ca81091b1.parquet	PARQUET	14225781	87598706	{1=3230817, 2=29547056, 3=12481143, 4=26796166, 5=7144115, 6=8398512}	{1=14225781, 2=14225781, 3=14225781, 4=14225781, 5=14225781, 6=14225781}	{1=0, 2=0, 3=447, 4=472, 5=0, 6=0}	{}	{1=7, 2=1, 3=1, 4=-0.0, 5=1, 6=1998-01-02T00:02:19}	{1=5999995, 2=203998, 3=100, 4=299.7, 5=15, 6=1998-12-31T23:59:25}			
3	0	s3://otfs-workshop-data-860010260849/datasets/athena_iceberg/web_sales_iceberg/data/z1t7Uw/ws_sales_time_year=1999/20250921_075743_00015_gcdxk-4f5f219c-29f7-4b40-86af-37909cc12c2d.parquet	PARQUET	14389905	88564033	{1=3253832, 2=29859797, 3=12625143, 4=27104159, 5=7226526, 6=8493679}	{1=14389905, 2=14389905, 3=14389905, 4=14389905, 5=14389905, 6=14389905}	{1=0, 2=0, 3=463, 4=473, 5=0, 6=0}	{}	{1=1, 2=1, 3=1, 4=-0.0, 5=1, 6=1999-01-01T00:01:37}	{1=5999996, 2=203998, 3=100, 4=300.0, 5=15, 6=1999-12-31T23:59:52}			
```

```sql
SELECT COUNT(DISTINCT(ws_order_number)) AS num_orders
FROM athena_iceberg_db.web_sales_iceberg
WHERE ws_sales_time >= TIMESTAMP '2000-01-01 00:00:00' AND ws_sales_time < TIMESTAMP '2000-02-01 00:00:00'
```

```bash
#	num_orders
1	69789
```

```sql
SELECT YEAR(ws_sales_time) as year, COUNT(*) AS records_per_year
FROM athena_iceberg_db.web_sales_iceberg
GROUP BY YEAR(ws_sales_time)
```

```bash
#	year	records_per_year
1	2000	14457568
2	1999	14389905
3	1998	14225781
```

クエリ文について分析する

```sql
EXPLAIN ANALYZE SELECT COUNT(DISTINCT(ws_order_number)) AS num_orders
FROM athena_iceberg_db.web_sales_iceberg
WHERE ws_sales_time >= TIMESTAMP '2000-01-01 00:00:00' AND ws_sales_time < TIMESTAMP '2000-02-01 00:00:00'
```

```bash
Query Plan
Queued: 256.68us, Analysis: 1.22s, Planning: 352.86ms, Execution: 2.78s
Fragment 1 [SINGLE]
    CPU: 1.13ms, Scheduled: 1.16ms, Blocked 10.63s (Input: 8.51s, Output: 0.00ns), Input: 8 rows (72B), Data Scanned: 0B; per task: avg.: 8.00 std.dev.: 0.00, Output: 1 row (9B)
    Output layout: [count]
    Output partitioning: SINGLE []
    Aggregate[type = FINAL]
    │   Layout: [count:bigint]
    │   Estimates: {rows: 1 (9B), cpu: 9, memory: 9B, network: 0B}
    │   CPU: 0.00ns (0.00%), Scheduled: 0.00ns (0.00%), Blocked: 0.00ns (0.00%), Output: 1 row (9B)
    │   Input avg.: 8.00 rows, Input std.dev.: 0.00%
    │   count := count("count_0")
    └─ LocalExchange[partitioning = SINGLE]
       │   Layout: [count_0:bigint]
       │   Estimates: {rows: 1 (9B), cpu: 0, memory: 0B, network: 0B}
       │   CPU: 0.00ns (0.00%), Scheduled: 0.00ns (0.00%), Blocked: 2.13s (4.82%), Output: 8 rows (72B)
       │   Input avg.: 2.00 rows, Input std.dev.: 35.36%
       └─ RemoteSource[sourceFragmentIds = [2]]
              Layout: [count_0:bigint]
              CPU: 0.00ns (0.00%), Scheduled: 0.00ns (0.00%), Blocked: 8.51s (19.27%), Output: 8 rows (72B)
              Input avg.: 2.00 rows, Input std.dev.: 35.36%

Fragment 2 [HASH]
    CPU: 107.07ms, Scheduled: 113.11ms, Blocked 33.54s (Input: 16.77s, Output: 0.00ns), Input: 69789 rows (340.77kB), Data Scanned: 0B; per task: avg.: 34894.50 std.dev.: 25.50, Output: 8 rows (72B)
    Output layout: [count_0]
    Output partitioning: SINGLE []
    Aggregate[type = PARTIAL]
    │   Layout: [count_0:bigint]
    │   Estimates: {rows: 1 (9B), cpu: ?, memory: 9B, network: 0B}
    │   CPU: 12.00ms (0.59%), Scheduled: 12.00ms (0.53%), Blocked: 0.00ns (0.00%), Output: 8 rows (72B)
    │   Input avg.: 8723.63 rows, Input std.dev.: 0.50%
    │   count_0 := count("ws_order_number")
    └─ Aggregate[type = FINAL, keys = [ws_order_number]]
       │   Layout: [ws_order_number:integer]
       │   Estimates: {rows: ? (?), cpu: ?, memory: ?, network: 0B}
       │   CPU: 91.00ms (4.46%), Scheduled: 96.00ms (4.26%), Blocked: 0.00ns (0.00%), Output: 69789 rows (340.77kB)
       │   Input avg.: 8723.63 rows, Input std.dev.: 0.50%
       └─ LocalExchange[partitioning = HASH, arguments = ["ws_order_number"]]
          │   Layout: [ws_order_number:integer]
          │   Estimates: {rows: ? (?), cpu: ?, memory: 0B, network: 0B}
          │   CPU: 3.00ms (0.15%), Scheduled: 3.00ms (0.13%), Blocked: 16.76s (37.94%), Output: 69789 rows (340.77kB)
          │   Input avg.: 8723.63 rows, Input std.dev.: 173.21%
          └─ RemoteSource[sourceFragmentIds = [3]]
                 Layout: [ws_order_number:integer]
                 CPU: 0.00ns (0.00%), Scheduled: 0.00ns (0.00%), Blocked: 16.77s (37.97%), Output: 69789 rows (340.77kB)
                 Input avg.: 8723.63 rows, Input std.dev.: 173.21%

Fragment 3 [SOURCE]
    CPU: 1.94s, Scheduled: 2.14s, Blocked 0.00ns (Input: 0.00ns, Output: 0.00ns), Input: 14457568 rows (193.03MB), Data Scanned: 11.26MB; per task: avg.: 7228784.00 std.dev.: 7228784.00, Output: 69789 rows (340.77kB)
    Amount of input data processed by the workers for this stage might be skewed
    Output layout: [ws_order_number]
    Output partitioning: HASH [ws_order_number]
    Aggregate[type = PARTIAL, keys = [ws_order_number]]
    │   Layout: [ws_order_number:integer]
    │   Estimates: {rows: ? (?), cpu: ?, memory: ?, network: 0B}
    │   CPU: 73.00ms (3.57%), Scheduled: 72.00ms (3.19%), Blocked: 0.00ns (0.00%), Output: 69789 rows (340.77kB)
    │   Input avg.: 418342.00 rows, Input std.dev.: 100.00%
    └─ ScanFilterProject[table = awsdatacatalog$iceberg-aws:athena_iceberg_db.web_sales_iceberg$data@1687392213482942033, filterPredicate = (("ws_sales_time" >= TIMESTAMP '2000-01-01 00:00:00.000000') AND ("ws_sales_time" < TIMESTAMP '2000-02-01 00:00:00.000000')), projectLocality = LOCAL, protectedBarrier = NONE]
           Layout: [ws_order_number:integer]
           Estimates: {rows: ? (?), cpu: ?, memory: 0B, network: 0B}/{rows: ? (?), cpu: ?, memory: 0B, network: 0B}/{rows: ? (?), cpu: ?, memory: 0B, network: 0B}
           CPU: 1.86s (91.23%), Scheduled: 2.07s (91.88%), Blocked: 0.00ns (0.00%), Output: 836684 rows (3.99MB)
           Input avg.: 7228784.00 rows, Input std.dev.: 100.00%
           ws_order_number := 1:ws_order_number:integer
           ws_sales_time := 6:ws_sales_time:timestamp(6)
           Input: 14457568 rows (193.03MB), Filtered: 94.21%, Physical input: 11.26MB, Physical input time: 240.59ms
```

## Apache Icebergテーブルへの条件付き更新、削除、または挿入

まずテーブルを作成する

```sql
CREATE TABLE athena_iceberg_db.merge_table (
    ws_order_number INT,
    ws_item_sk INT,
    ws_quantity INT,
    ws_sales_price DOUBLE,
    ws_warehouse_sk INT,
    ws_sales_time TIMESTAMP,
    operation string)
PARTITIONED BY (year(ws_sales_time))
LOCATION 's3://otfs-workshop-data-860010260849/datasets/athena_iceberg/merge_table'
TBLPROPERTIES (
  'table_type'='iceberg',
  'format'='PARQUET',
  'write_compression'='ZSTD'
);
```

You have the column operation in the merge_table. Now you add records inside this table and add the column value operation = 'U' to identify records to be updated, operation = 'I' to identify records to be inserted and operation = 'D' to identify records to be deleted.

更新してみる

```sql
INSERT INTO athena_iceberg_db.merge_table
SELECT ws_order_number, ws_item_sk, ws_quantity, ws_sales_price, 16 AS ws_warehouse_sk, ws_sales_time, 'U' as operation  
FROM tpcds.prepared_web_sales where year(ws_sales_time) = 2000 AND ws_warehouse_sk = 10 
```

Insertしてみる

```sql
INSERT INTO athena_iceberg_db.merge_table
SELECT ws_order_number, ws_item_sk, ws_quantity, ws_sales_price, ws_warehouse_sk, ws_sales_time, 'I' as operation
FROM tpcds.prepared_web_sales where year(ws_sales_time) = 2001
```

削除してみる

```sql
INSERT INTO athena_iceberg_db.merge_table
SELECT ws_order_number, ws_item_sk, ws_quantity, ws_sales_price, ws_warehouse_sk, ws_sales_time, 'D' as operation  
FROM tpcds.prepared_web_sales where year(ws_sales_time) = 1999 AND ws_warehouse_sk = 9
```

オペレーションについてクエリしてみる

```sql
select operation, count(*) as num_records
from athena_iceberg_db.merge_table
group by operation
```

```bash
#	operation	num_records
1	D	960104
2	I	14334677
3	U	963852
```

全ての変更を適用してみる

```bash
MERGE INTO athena_iceberg_db.web_sales_iceberg t
USING athena_iceberg_db.merge_table s
    ON t.ws_order_number = s.ws_order_number AND t.ws_item_sk = s.ws_item_sk
WHEN MATCHED AND s.operation like 'D' THEN DELETE
WHEN MATCHED AND s.operation like 'U' THEN UPDATE SET ws_order_number = s.ws_order_number, ws_item_sk = s.ws_item_sk, ws_quantity = s.ws_quantity, ws_sales_price = s.ws_sales_price, ws_warehouse_sk = s.ws_warehouse_sk, ws_sales_time = s.ws_sales_time
WHEN NOT MATCHED THEN INSERT (ws_order_number, ws_item_sk, ws_quantity, ws_sales_price, ws_warehouse_sk, ws_sales_time) VALUES (s.ws_order_number, s.ws_item_sk, s.ws_quantity, s.ws_sales_price, s.ws_warehouse_sk, s.ws_sales_time)
```

変更がマージされた後に対してそれぞれクエリしてみる

```sql
SELECT YEAR(ws_sales_time) AS year, COUNT(*) as records_per_year
FROM athena_iceberg_db.web_sales_iceberg
GROUP BY (YEAR(ws_sales_time))
ORDER BY year
```

```bash
#	year	records_per_year
1	1998	14225781
2	1999	13429801
3	2000	14457568
4	2001	14334677
```

```sql
SELECT ws_warehouse_sk, COUNT(*) as records_per_warehouse
FROM athena_iceberg_db.web_sales_iceberg
WHERE YEAR(ws_sales_time) = 2000
GROUP BY ws_warehouse_sk
ORDER BY ws_warehouse_sk
```

```bash
#	ws_warehouse_sk	records_per_warehouse
1	1	964133
2	2	965386
3	3	964568
4	4	963502
5	5	964424
6	6	962453
7	7	963496
8	8	963020
9	9	963695
10	11	964545
11	12	963286
12	13	965135
13	14	962938
14	15	963135
15	16	963852
```

```sql
SELECT ws_warehouse_sk, COUNT(*) as records_per_warehouse
FROM athena_iceberg_db.web_sales_iceberg
WHERE YEAR(ws_sales_time) = 1999
GROUP BY ws_warehouse_sk
ORDER BY ws_warehouse_sk
```

```bash
#	ws_warehouse_sk	records_per_warehouse
1	1	959049
2	2	959863
3	3	958385
4	4	960963
5	5	957628
6	6	958349
7	7	960763
8	8	957587
9	10	958569
10	11	959535
11	12	960991
12	13	960091
13	14	959176
14	15	958852
```

## Icebergテーブルの最適化

Icebergテーブルにデータが蓄積されるにつれて、開く必要のあるファイル数が増加するため、クエリの効率は徐々に低下します。さらに、テーブルに**削除ファイル（delete files）**が含まれている場合、追加の計算コストが発生します。Icebergでは、削除ファイルに行レベルの削除情報が保存されるため、クエリ結果を返す前にエンジンがこれらの削除を適用しなければならないためです。

Icebergテーブルでのクエリパフォーマンスを最適化するために、Athenaはテーブルメンテナンスコマンドとして手動コンパクションをサポートしています。コンパクションは、テーブルの内容を変更することなく、構造的なレイアウトを最適化します。これにはOPTIMIZE Athenaクエリを使用し、以下のことを実行します。

小さなファイルをより大きなファイルに統合する（読み取り時に開くファイル数を削減する）

ポジション削除ファイルをデータファイルにマージする（クエリ時にポジション削除を適用する手間を省く）

テーブルを最適化する前に、Icebergのデータファイルを確認しておきましょう。

```sql
SELECT * FROM "athena_iceberg_db"."web_sales_iceberg$files";
```

```bash
#	content	file_path	file_format	record_count	file_size_in_bytes	column_sizes	value_counts	null_value_counts	nan_value_counts	lower_bounds	upper_bounds	key_metadata	split_offsets	equality_ids
1	0	s3://otfs-workshop-data-860010260849/datasets/athena_iceberg/web_sales_iceberg/data/5rzfPw/ws_sales_time_year=2000/20250921_080604_00127_snrdp-9bd8f9a9-976d-4bcb-bfbb-6454edb80f3a.parquet	PARQUET	963852	13247765	{1=2807473, 2=2344094, 3=845800, 4=1886888, 5=206, 6=5362445}	{1=963852, 2=963852, 3=963852, 4=963852, 5=963852, 6=963852}	{1=0, 2=0, 3=22, 4=25, 5=0, 6=0}	{}	{1=3, 2=1, 3=1, 4=-0.0, 5=16, 6=2000-01-01T00:01:03}	{1=6000000, 2=203999, 3=100, 4=296.61, 5=16, 6=2000-12-31T23:56:24}			
2	0	s3://otfs-workshop-data-860010260849/datasets/athena_iceberg/web_sales_iceberg/data/wjOaGw/ws_sales_time_year=2001/20250921_080604_00127_snrdp-2bc70350-92eb-4a05-8248-835f2c8167b5.parquet	PARQUET	14334677	148245552	{1=19271391, 2=31062826, 3=12576824, 4=27079776, 5=7198858, 6=51054332}	{1=14334677, 2=14334677, 3=14334677, 4=14334677, 5=14334677, 6=14334677}	{1=0, 2=0, 3=450, 4=452, 5=0, 6=0}	{}	{1=5, 2=1, 3=1, 4=-0.0, 5=1, 6=2001-01-01T00:00:03}	{1=5999999, 2=203999, 3=100, 4=299.73, 5=15, 6=2001-12-31T23:59:46}			
3	0	s3://otfs-workshop-data-860010260849/datasets/athena_iceberg/web_sales_iceberg/data/UHn_bw/ws_sales_time_year=2000/20250921_075743_00015_gcdxk-1b26154d-4f84-4186-b850-8179e0137246.parquet	PARQUET	14457568	89032038	{1=3261796, 2=30044555, 3=12684325, 4=27231071, 5=7260491, 6=8548903}	{1=14457568, 2=14457568, 3=14457568, 4=14457568, 5=14457568, 6=14457568}	{1=0, 2=0, 3=424, 4=416, 5=0, 6=0}	{}	{1=3, 2=1, 3=1, 4=-0.0, 5=1, 6=2000-01-01T00:01:03}	{1=6000000, 2=203999, 3=100, 4=299.7, 5=15, 6=2000-12-31T23:59:20}			
4	0	s3://otfs-workshop-data-860010260849/datasets/athena_iceberg/web_sales_iceberg/data/tGtcdw/ws_sales_time_year=1998/20250921_075743_00015_gcdxk-d30624a0-3f45-4eaa-ba23-d96ca81091b1.parquet	PARQUET	14225781	87598706	{1=3230817, 2=29547056, 3=12481143, 4=26796166, 5=7144115, 6=8398512}	{1=14225781, 2=14225781, 3=14225781, 4=14225781, 5=14225781, 6=14225781}	{1=0, 2=0, 3=447, 4=472, 5=0, 6=0}	{}	{1=7, 2=1, 3=1, 4=-0.0, 5=1, 6=1998-01-02T00:02:19}	{1=5999995, 2=203998, 3=100, 4=299.7, 5=15, 6=1998-12-31T23:59:25}			
5	0	s3://otfs-workshop-data-860010260849/datasets/athena_iceberg/web_sales_iceberg/data/z1t7Uw/ws_sales_time_year=1999/20250921_075743_00015_gcdxk-4f5f219c-29f7-4b40-86af-37909cc12c2d.parquet	PARQUET	14389905	88564033	{1=3253832, 2=29859797, 3=12625143, 4=27104159, 5=7226526, 6=8493679}	{1=14389905, 2=14389905, 3=14389905, 4=14389905, 5=14389905, 6=14389905}	{1=0, 2=0, 3=463, 4=473, 5=0, 6=0}	{}	{1=1, 2=1, 3=1, 4=-0.0, 5=1, 6=1999-01-01T00:01:37}	{1=5999996, 2=203998, 3=100, 4=300.0, 5=15, 6=1999-12-31T23:59:52}			
```

最適化を実行する

```sql
OPTIMIZE athena_iceberg_db.web_sales_iceberg REWRITE DATA USING BIN_PACK;
```

ちなみに以下のように条件を指定して最適化もできる

```sql
OPTIMIZE athena_iceberg_db.web_sales_iceberg REWRITE DATA USING BIN_PACK
where year(ws_sales_time) = 2000
```

最適化後に再度取得

```sql
SELECT * FROM "athena_iceberg_db"."web_sales_iceberg$files";
```

```bash
#	content	file_path	file_format	record_count	file_size_in_bytes	column_sizes	value_counts	null_value_counts	nan_value_counts	lower_bounds	upper_bounds	key_metadata	split_offsets	equality_ids
1	0	s3://otfs-workshop-data-860010260849/datasets/athena_iceberg/web_sales_iceberg/data/GUt8gw/ws_sales_time_year=2000/20250921_081042_00103_p2uni-63b20b03-5313-479b-99f1-a192da49a7ba.parquet	PARQUET	14457568	95991448	{1=4928766, 2=30693913, 3=12684309, 4=27230920, 5=6776628, 6=13676034}	{1=14457568, 2=14457568, 3=14457568, 4=14457568, 5=14457568, 6=14457568}	{1=0, 2=0, 3=424, 4=416, 5=0, 6=0}	{}	{1=3, 2=1, 3=1, 4=-0.0, 5=1, 6=2000-01-01T00:01:03}	{1=6000000, 2=203999, 3=100, 4=299.7, 5=16, 6=2000-12-31T23:59:20}			
2	0	s3://otfs-workshop-data-860010260849/datasets/athena_iceberg/web_sales_iceberg/data/6HTRng/ws_sales_time_year=1998/20250921_081042_00103_p2uni-87c1880b-2416-46a3-b21c-8b5cd06e6ebb.parquet	PARQUET	14225781	87662457	{1=3233586, 2=29609717, 3=12481187, 4=26795954, 5=7144052, 6=8397064}	{1=14225781, 2=14225781, 3=14225781, 4=14225781, 5=14225781, 6=14225781}	{1=0, 2=0, 3=447, 4=472, 5=0, 6=0}	{}	{1=7, 2=1, 3=1, 4=-0.0, 5=1, 6=1998-01-02T00:02:19}	{1=5999995, 2=203998, 3=100, 4=299.7, 5=15, 6=1998-12-31T23:59:25}			
3	0	s3://otfs-workshop-data-860010260849/datasets/athena_iceberg/web_sales_iceberg/data/oPn4tA/ws_sales_time_year=1999/20250921_081042_00103_p2uni-267ff7e6-979e-456f-a5ec-03623566533e.parquet	PARQUET	13429801	84295189	{1=3553811, 2=28303515, 3=11782836, 4=25301549, 5=6744338, 6=8608243}	{1=13429801, 2=13429801, 3=13429801, 4=13429801, 5=13429801, 6=13429801}	{1=0, 2=0, 3=443, 4=451, 5=0, 6=0}	{}	{1=1, 2=1, 3=1, 4=-0.0, 5=1, 6=1999-01-01T00:01:37}	{1=5999996, 2=203998, 3=100, 4=300.0, 5=15, 6=1999-12-31T23:59:52}			
4	0	s3://otfs-workshop-data-860010260849/datasets/athena_iceberg/web_sales_iceberg/data/wjOaGw/ws_sales_time_year=2001/20250921_080604_00127_snrdp-2bc70350-92eb-4a05-8248-835f2c8167b5.parquet	PARQUET	14334677	148245552	{1=19271391, 2=31062826, 3=12576824, 4=27079776, 5=7198858, 6=51054332}	{1=14334677, 2=14334677, 3=14334677, 4=14334677, 5=14334677, 6=14334677}	{1=0, 2=0, 3=450, 4=452, 5=0, 6=0}	{}	{1=5, 2=1, 3=1, 4=-0.0, 5=1, 6=2001-01-01T00:00:03}	{1=5999999, 2=203999, 3=100, 4=299.73, 5=15, 6=2001-12-31T23:59:46}			
```

各パーティションへの影響
- ws_sales_time_year=1998パーティション: このパーティションは変更していないため、ファイル数は変わりません。
- ws_sales_time_year=1999パーティション: Mergeコマンド（ステップ6.3）でレコードが削除されたため、レコード数が減っています。
- ws_sales_time_year=2000パーティション: Mergeコマンド（ステップ6.1）でレコードが更新されたため、2つのファイルが1つにコンパクションされました。データファイルがポジション削除ファイルとマージされた結果です。
- ws_sales_time_year=2001パーティション: このパーティションにはレコードを挿入しただけなので、ファイル数は変わりません。

新たなテーブルを作成する

```sql
CREATE TABLE athena_iceberg_db.web_sales_iceberg (
ws_order_number INT,
ws_item_sk INT,
ws_quantity INT,
ws_sales_price DOUBLE,
ws_warehouse_sk INT,
ws_sales_time TIMESTAMP)
PARTITIONED BY (year(ws_sales_time))
LOCATION 's3://otfs-workshop-data-860010260849/datasets/athena_iceberg/web_sales_iceberg'
TBLPROPERTIES (
'table_type'='iceberg',
'format'='PARQUET',
'write_compression'='ZSTD',
'write_target_data_file_size_bytes'='346870912',
'optimize_rewrite_delete_file_threshold'='16',
'optimize_rewrite_data_file_threshold'='16'
);
```

##  Icebergテーブルの利用
このラボでは、Amazon AthenaとAmazon Redshiftを使用してIcebergテーブルにクエリを実行する方法を学びます。

Amazon Athenaは、Apache Icebergテーブルに対する読み込み、タイムトラベル、書き込み、およびDDLクエリをサポートしています。

また、Redshift Spectrumを使用すると、AWS Glueデータカタログに登録されたApache Icebergテーブルにクエリを実行できます。Amazon Redshiftは、Apache Icebergのメタデータに保存されているテーブルの統計情報を利用して、クエリプランを最適化し、クエリ処理中のファイルスキャンを減らすことができます。Amazon Redshift SQLを使えば、Redshiftのテーブルとデータレイクのテーブルを結合することができます。

このラボでは、Apache IcebergをAmazonの分析サービスでどのように活用できるかに焦点を当てているようですね。RedshiftでのIcebergの活用方法について、さらに詳しく知りたいことはありますか？

```sql
SELECT ws_warehouse_sk, count(distinct(ws_order_number)) as num_orders
FROM web_sales_iceberg
WHERE ws_warehouse_sk in (5,6,10,11)
GROUP BY ws_warehouse_sk
```

```bash
#	ws_warehouse_sk	num_orders
1	6	2658138
2	10	1990680
3	11	2661887
4	5	2661039
```

```sql
SELECT count(*)
FROM customer_iceberg
```

```bash
#	_col0
1	1999999
```

```bash
Query Plan
Fragment 0 [SINGLE]
    Output layout: [count]
    Output partitioning: SINGLE []
    Output[columnNames = [_col0]]
    │   Layout: [count:bigint]
    │   Estimates: {rows: 1 (9B), cpu: 0, memory: 0B, network: 0B}
    │   _col0 := count
    └─ Aggregate[type = FINAL]
       │   Layout: [count:bigint]
       │   Estimates: {rows: 1 (9B), cpu: 9, memory: 9B, network: 0B}
       │   count := count("count_0")
       └─ LocalExchange[partitioning = SINGLE]
          │   Layout: [count_0:bigint]
          │   Estimates: {rows: 1 (9B), cpu: 0, memory: 0B, network: 0B}
          └─ RemoteSource[sourceFragmentIds = [1]]
                 Layout: [count_0:bigint]

Fragment 1 [SOURCE]
    Output layout: [count_0]
    Output partitioning: SINGLE []
    Aggregate[type = PARTIAL]
    │   Layout: [count_0:bigint]
    │   Estimates: {rows: 1 (9B), cpu: 0, memory: 9B, network: 0B}
    │   count_0 := count(*)
    └─ TableScan[table = awsdatacatalog$iceberg-aws:athena_iceberg_db.customer_iceberg$data@5430518374707242081]
           Layout: []
           Estimates: {rows: ? (0B), cpu: 0, memory: 0B, network: 0B}
```

```sql
EXPLAIN ANALYZE
SELECT ws_warehouse_sk, count(distinct(ws_order_number)) as num_orders
FROM web_sales_iceberg
WHERE ws_warehouse_sk in (5,6,10,11)
GROUP BY ws_warehouse_sk
```

```bash
Query Plan
Queued: 1.03ms, Analysis: 1.53s, Planning: 459.05ms, Execution: 4.42s
Fragment 1 [HASH]
    CPU: 6.97ms, Scheduled: 7.21ms, Blocked 1.46m (Input: 43.92s, Output: 0.00ns), Input: 48 rows (672B), Data Scanned: 0B; per task: avg.: 16.00 std.dev.: 5.66, Output: 4 rows (56B)
    Output layout: [ws_warehouse_sk, count]
    Output partitioning: SINGLE []
    Aggregate[type = FINAL, keys = [ws_warehouse_sk]]
    │   Layout: [ws_warehouse_sk:integer, count:bigint]
    │   Estimates: {rows: ? (?), cpu: ?, memory: ?, network: 0B}
    │   CPU: 2.00ms (0.01%), Scheduled: 2.00ms (0.01%), Blocked: 0.00ns (0.00%), Output: 4 rows (56B)
    │   Input avg.: 4.00 rows, Input std.dev.: 187.08%
    │   count := count("count_0")
    └─ LocalExchange[partitioning = HASH, arguments = ["ws_warehouse_sk"]]
       │   Layout: [ws_warehouse_sk:integer, count_0:bigint]
       │   Estimates: {rows: ? (?), cpu: ?, memory: 0B, network: 0B}
       │   CPU: 0.00ns (0.00%), Scheduled: 0.00ns (0.00%), Blocked: 43.92s (26.07%), Output: 48 rows (672B)
       │   Input avg.: 4.00 rows, Input std.dev.: 36.80%
       └─ RemoteSource[sourceFragmentIds = [2]]
              Layout: [ws_warehouse_sk:integer, count_0:bigint]
              CPU: 0.00ns (0.00%), Scheduled: 0.00ns (0.00%), Blocked: 43.92s (26.07%), Output: 48 rows (672B)
              Input avg.: 4.00 rows, Input std.dev.: 36.80%

Fragment 2 [HASH]
    CPU: 4.44s, Scheduled: 4.47s, Blocked 1.34m (Input: 40.89s, Output: 0.00ns), Input: 10889196 rows (103.85MB), Data Scanned: 0B; per task: avg.: 3629732.00 std.dev.: 134.55, Output: 48 rows (672B)
    Output layout: [ws_warehouse_sk, count_0]
    Output partitioning: HASH [ws_warehouse_sk]
    Aggregate[type = PARTIAL, keys = [ws_warehouse_sk]]
    │   Layout: [ws_warehouse_sk:integer, count_0:bigint]
    │   Estimates: {rows: ? (?), cpu: ?, memory: ?, network: 0B}
    │   CPU: 1.26s (8.04%), Scheduled: 1.28s (7.01%), Blocked: 0.00ns (0.00%), Output: 48 rows (672B)
    │   Input avg.: 830978.67 rows, Input std.dev.: 0.11%
    │   count_0 := count("ws_order_number")
    └─ Aggregate[type = FINAL, keys = [ws_warehouse_sk, ws_order_number]]
       │   Layout: [ws_warehouse_sk:integer, ws_order_number:integer]
       │   Estimates: {rows: ? (?), cpu: ?, memory: ?, network: 0B}
       │   CPU: 2.30s (14.63%), Scheduled: 2.30s (12.59%), Blocked: 0.00ns (0.00%), Output: 9971744 rows (95.10MB)
       │   Input avg.: 907433.00 rows, Input std.dev.: 0.13%
       └─ LocalExchange[partitioning = HASH, arguments = ["ws_warehouse_sk", "ws_order_number"]]
          │   Layout: [ws_warehouse_sk:integer, ws_order_number:integer]
          │   Estimates: {rows: ? (?), cpu: ?, memory: 0B, network: 0B}
          │   CPU: 840.00ms (5.35%), Scheduled: 841.00ms (4.60%), Blocked: 39.74s (23.59%), Output: 10889196 rows (103.85MB)
          │   Input avg.: 907433.00 rows, Input std.dev.: 3.30%
          └─ RemoteSource[sourceFragmentIds = [3]]
                 Layout: [ws_warehouse_sk:integer, ws_order_number:integer]
                 CPU: 21.00ms (0.13%), Scheduled: 21.00ms (0.11%), Blocked: 40.89s (24.27%), Output: 10889196 rows (103.85MB)
                 Input avg.: 907433.00 rows, Input std.dev.: 3.30%

Fragment 3 [SOURCE]
    CPU: 11.29s, Scheduled: 13.83s, Blocked 0.00ns (Input: 0.00ns, Output: 0.00ns), Input: 56447827 rows (533.76MB), Data Scanned: 56.13MB; per task: avg.: 18815942.33 std.dev.: 12278566.38, Output: 10889196 rows (103.85MB)
    Output layout: [ws_warehouse_sk, ws_order_number]
    Output partitioning: HASH [ws_warehouse_sk, ws_order_number]
    Aggregate[type = PARTIAL, keys = [ws_warehouse_sk, ws_order_number]]
    │   Layout: [ws_warehouse_sk:integer, ws_order_number:integer]
    │   Estimates: {rows: ? (?), cpu: ?, memory: ?, network: 0B}
    │   CPU: 4.53s (28.87%), Scheduled: 4.53s (24.80%), Blocked: 0.00ns (0.00%), Output: 10889196 rows (103.85MB)
    │   Input avg.: 1593510.67 rows, Input std.dev.: 107.79%
    └─ ScanFilter[table = awsdatacatalog$iceberg-aws:athena_iceberg_db.web_sales_iceberg$data@2809973934752229823, filterPredicate = ("ws_warehouse_sk" IN (5, 6, 10, 11))]
           Layout: [ws_order_number:integer, ws_warehouse_sk:integer]
           Estimates: {rows: ? (?), cpu: ?, memory: 0B, network: 0B}/{rows: ? (?), cpu: ?, memory: 0B, network: 0B}
           CPU: 6.75s (42.97%), Scheduled: 9.29s (50.87%), Blocked: 0.00ns (0.00%), Output: 14341596 rows (136.77MB)
           Input avg.: 6271980.78 rows, Input std.dev.: 107.06%
           ws_warehouse_sk := 5:ws_warehouse_sk:integer
           ws_order_number := 1:ws_order_number:integer
           Input: 56447827 rows (533.76MB), Filtered: 74.59%, Physical input: 56.13MB, Physical input time: 2.50s
```

To create and query Athena views on Iceberg tables, use the CREATE VIEW statement. Copy the query below into the query editor and click Run.

```sql
CREATE VIEW total_orders_by_warehouse
AS
SELECT ws_warehouse_sk, count(distinct(ws_order_number)) as num_orders
FROM web_sales_iceberg
WHERE ws_warehouse_sk in (5,6,10,11)
GROUP BY ws_warehouse_sk
```

以下で取得

```sql
SELECT *
FROM total_orders_by_warehouse
```

```bash
#	ws_warehouse_sk	num_orders
1	6	2658138
2	10	1990680
3	5	2661039
4	11	2661887

```