SET mapreduce.job.queuename=ddsw;
SET hive.exec.drop.ignorenonexistent=true;
SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;

CREATE DATABASE IF NOT EXISTS ddsw_dev;

USE ddsw_dev;

DROP TABLE IF EXISTS DEALER_HIERARCHY_CURRENT;

CREATE EXTERNAL TABLE DEALER_HIERARCHY_CURRENT
    LIKE DEALER_HIERARCHY_CORE
    LOCATION '${hiveconf:hadoop.tmp.dir.uri}/dealer_hierarchy/current_data';

ALTER TABLE DEALER_HIERARCHY_CURRENT SET FILEFORMAT PARQUET;

ALTER TABLE DEALER_HIERARCHY_CURRENT SET SERDEPROPERTIES('avro.schema.url'='none');