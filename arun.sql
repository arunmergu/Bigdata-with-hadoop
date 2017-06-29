drop table test.txnrecords;
drop database test;
create database test;
use test;
create table test.txnrecords(txnno INT, txndate STRING, custno INT, amount DOUBLE, 
category STRING, product STRING, city STRING, state STRING, spendby STRING)
row format delimited
fields terminated by ','
stored as textfile;
LOAD DATA LOCAL INPATH '/home/hduser/txns1.txt' OVERWRITE INTO TABLE txnrecords;
create table test.txnrecsByCat(txnno INT, txndate STRING, custno INT, amount DOUBLE,
product STRING, city STRING, state STRING, spendby STRING)
partitioned by (category STRING)
clustered by (state) into 10 buckets
row format delimited
fields terminated by ','
stored as textfile;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.dynamic.partition=true;
set hive.enforce.bucketing=true;
from test.txnrecords txn INSERT OVERWRITE TABLE test.txnrecsByCat PARTITION(category) select txn.txnno, txn.txndate,txn.custno, txn.amount,txn.product,txn.city,txn.state, txn.spendby, txn.category DISTRIBUTE By category;

