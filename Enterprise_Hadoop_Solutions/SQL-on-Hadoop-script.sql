--Impala优化参数
set mem_limit=64g;
set DISABLE_UNSAFE_SPILLS=true;
set parquet_file_size=400m;
set RESERVATION_REQUEST_TIMEOUT=900000;

--性能测试SQL
1:
SELECT COUNT(*) FROM customers WHERE name = 'Asher MATTHEWS';

--Drill
select count(*) from customers where customers.info.name = 'Asher MATTHEWS';

2:
SELECT category, count(*) cnt FROM books GROUP BY category ORDER BY cnt DESC LIMIT 10;

--Drill
select cast(books.info.category as VarChar(20)),count(*) cnt from books GROUP by cast(books.info.category as VarChar(20)) order by cnt desc limit 10;

3:
SELECT tmp.book_category, ROUND(tmp.revenue, 2) AS revenue
FROM (
  SELECT books.category AS book_category, SUM(books.price * transactions.quantity) AS revenue
  FROM books JOIN transactions ON (
    transactions.book_id = books.id
    AND YEAR(transactions.transaction_date) BETWEEN 2008 AND 2010
  )
  GROUP BY books.category
) tmp
ORDER BY revenue DESC LIMIT 10;

4: 
SELECT tmp.book_category, ROUND(tmp.revenue, 2) AS revenue
FROM (
  SELECT books.category AS book_category, SUM(books.price * transactions.quantity) AS revenue
  FROM books
  JOIN transactions ON (
    transactions.book_id = books.id
  )
  JOIN customers ON (
    transactions.customer_id = customers.id
    AND customers.state IN ('WA', 'CA', 'NY')
  )
  GROUP BY books.category
) tmp
ORDER BY revenue DESC LIMIT 10;

--Drill
SELECT tmp.book_category, ROUND(tmp.revenue, 2) AS revenue
FROM (
  SELECT cast(books.info.category as varchar(20)) AS book_category, SUM(cast(books.info.price as float) * cast(transactions.info.quantity as bigint)) AS revenue
  FROM books
  JOIN transactions ON (
    transactions.row_key = books.row_key
  )
  JOIN customers ON (
    cast(transactions.info.customer_id as varchar(20)) = cast(customers.row_key as VARCHAR(20))
    AND cast(customers.info.state as varchar(20)) IN ('WA', 'CA', 'NY')
  )
  GROUP BY books.info.category
) tmp
ORDER BY revenue DESC LIMIT 10;

5:
SELECT tmp.book_category, ROUND(tmp.revenue, 2) AS revenue
  FROM (
    SELECT books.category AS book_category, SUM(books.price * transactions.quantity) AS revenue
    FROM books JOIN transactions ON (
      transactions.book_id = books.id
    )
    GROUP BY books.category
  ) tmp
ORDER BY revenue DESC LIMIT 10;

--Drill
SELECT tmp.book_category, ROUND(tmp.revenue, 2) AS revenue
  FROM (
    SELECT cast(books.info.category as varchar(20)) AS book_category, SUM(cast(books.info.price as float) * cast(transactions.info.quantity as bigint)) AS revenue
    FROM books JOIN transactions ON (
      cast(transactions.info.book_id as varchar(20)) = cast(books.row_key as varchar(20))
    )
    GROUP BY books.info.category
  ) tmp
ORDER BY revenue DESC LIMIT 10;


--创建hbase snappy表
create 'books', { NAME => 'info', COMPRESSION => 'snappy' }
create 'customers', { NAME => 'info', COMPRESSION => 'snappy' }
create 'transactions', { NAME => 'info', COMPRESSION => 'snappy' }

--load数据到hbase表
sudo -uhdfs hbase org.apache.hadoop.hbase.mapreduce.ImportTsv -Dimporttsv.separator="|" -Dimporttsv.columns=HBASE_ROW_KEY,info:isbn,info:category,info:publish_date,info:publisher,info:price -Dimporttsv.bulk.output=/tmp/hbase/books books /data/books/books

sudo -uhdfs hbase org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles /tmp/hbase/books books  

sudo -uhdfs hbase org.apache.hadoop.hbase.mapreduce.ImportTsv -Dimporttsv.separator="|" -Dimporttsv.columns=HBASE_ROW_KEY,info:name,info:date_of_birth,info:gender,info:state,info:email,info:phone -Dimporttsv.bulk.output=/tmp/hbase/customers customers /data/customers/customers

sudo -uhdfs hbase org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles /tmp/hbase/customers customers

sudo -uhdfs hbase org.apache.hadoop.hbase.mapreduce.ImportTsv -Dimporttsv.separator="|" -Dimporttsv.columns=HBASE_ROW_KEY,info:customer_id,info:book_id,info:quantity,info:transaction_date -Dimporttsv.bulk.output=/tmp/hbase/transactions transactions /data/transactions/transactions

sudo -uhdfs hbase org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles /tmp/hbase/transactions transactions

--hbase split regions
 major_compact 'books'
 major_compact 'customers'
 major_compact 'transactions'
 split 'transactions'

 --phoenix load数据到hbase表
CREATE  TABLE books(
  id bigint,
  isbn varchar,
  category varchar,
  publish_date TIMESTAMP,
  publisher varchar,
  price float
)
 
CREATE TABLE customers(
  id bigint,
  name varchar,
  date_of_birth TIMESTAMP,
  gender varchar,
  state varchar,
  email varchar,
  phone varchar
)

CREATE TABLE transactions(
  id bigint,
  customer_id bigint,
  book_id bigint,
  quantity bigint,
  transaction_date TIMESTAMP
)

hadoop jar /opt/cloudera/parcels/CLABS_PHOENIX-4.5.2-1.clabs_phoenix1.1.0.p0.687/lib/phoenix/phoenix-1.1.0-client.jar org.apache.phoenix.mapreduce.CsvBulkLoadTool -d '|' --table BOOKS --input /data/books/books 

hadoop jar /opt/cloudera/parcels/CLABS_PHOENIX-4.5.2-1.clabs_phoenix1.1.0.p0.687/lib/phoenix/phoenix-1.1.0-client.jar org.apache.phoenix.mapreduce.CsvBulkLoadTool -d '|' --table CUSTOMERS --input /data/customers/customers 

hadoop jar /opt/cloudera/parcels/CLABS_PHOENIX-4.5.2-1.clabs_phoenix1.1.0.p0.687/lib/phoenix/phoenix-1.1.0-client.jar org.apache.phoenix.mapreduce.CsvBulkLoadTool -d '|' --table TRANSACTIONS --input /data/transactions/transactions 

--hive mapping hbase table
CREATE EXTERNAL TABLE default.books_hbase(
  id BIGINT,
  isbn STRING,
  category STRING,
  publish_date TIMESTAMP,
  publisher STRING,
  price FLOAT
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.hbase.HBaseSerDe'
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES ("hbase.columns.mapping" = ":key , info:isbn, info:category, info:publish_date, info:publisher, info:price")
TBLPROPERTIES("hbase.table.name" = "books");

CREATE EXTERNAL TABLE default.customers_hbase(
  id BIGINT,
  name STRING,
  date_of_birth TIMESTAMP,
  gender STRING,
  state STRING,
  email STRING,
  phone STRING
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.hbase.HBaseSerDe'
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES ("hbase.columns.mapping" = ":key , info:name, info:date_of_birth, info:gender, info:state, info:email, info:phone")
TBLPROPERTIES("hbase.table.name" = "customers");

CREATE EXTERNAL TABLE default.transactions_hbase(
  id BIGINT,
  customer_id BIGINT,
  book_id BIGINT,
  quantity INT,
  transaction_date TIMESTAMP
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.hbase.HBaseSerDe'
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES ("hbase.columns.mapping" = ":key , info:customer_id, info:book_id, info:quantity, info:transaction_date")
TBLPROPERTIES("hbase.table.name" = "transactions");

--phoenix mapping hbase table

CREATE VIEW "books" ( id VARCHAR primary key, "isbn" VARCHAR, "category" VARCHAR,"publish_date" VARCHAR, "publisher" VARCHAR, "price" VARCHAR ) default_column_family='info';

select * from "books" limit 10;

SELECT "books"."category", count(*) cnt FROM "books" GROUP BY "books"."category" ORDER BY cnt DESC LIMIT 10;

----hbase shell
scan 'books', {LIMIT => 10}
scan 'books', {COLUMNS => ['info:ISBN', 'info:PRICE'], LIMIT => 10}


----Drill query hbsae binary data
SELECT CAST(students.clickinfo.studentid as VarChar(20)), CAST(students.account.name as VarChar(20)), CAST (students.address.state as VarChar(20)), CAST (students.address.street as VarChar(20)), CAST (students.address.zipcode as VarChar(20)), FROM hbase.students;

select cast(row_key as VarChar(20)) from books limit 10;

select cast(books.row_key as VarChar(20)),cast(books.info.category as VarChar(20)) from hbase.books limit 10;