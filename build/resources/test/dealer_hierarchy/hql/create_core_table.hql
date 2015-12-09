SET mapreduce.job.queuename=ddsw;
SET hive.exec.drop.ignorenonexistent=true;
SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;

USE ddsw_dev;

CREATE EXTERNAL TABLE DEALER_HIERARCHY_CORE
    PARTITIONED BY (dealer_code string)
    ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.avro.AvroSerDe'
    WITH SERDEPROPERTIES
    (
        'avro.schema.url'='./src/test/resources/dealer_hierarchy/schema/DEALER_HIERARCHY_CORE.avsc'
    )
    STORED AS
        INPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat'
        OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat'
    LOCATION '${hiveconf:hadoop.tmp.dir.uri}/dealer_hierarchy/core_data';
