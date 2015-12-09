SET mapreduce.job.queuename=ddsw;
SET hive.exec.drop.ignorenonexistent=true;
SET hive.exec.dynamic.partition.mode=nonstrict;

CREATE DATABASE IF NOT EXISTS ddsw_dev;

USE ddsw_dev;

CREATE EXTERNAL TABLE IF NOT EXISTS ddsw_dev.dealer_hierarchy_xmlvalidationerrors
(
    filename string,
    dealercode string,
    tablekey string,
    tablekeyvalue string,
    errormessage string,
    invalidxml string,
    xmlrecord string,
    createdon string
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u0001'
STORED AS INPUTFORMAT'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION '${hiveconf:hadoop.tmp.dir.uri}/dealer_hierarchy'
TBLPROPERTIES ('transient_lastDdlTime'='1426009340');
