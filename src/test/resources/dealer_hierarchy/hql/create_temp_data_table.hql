SET mapreduce.job.queuename=ddsw;
SET hive.exec.drop.ignorenonexistent=true;
SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;

CREATE DATABASE IF NOT EXISTS ddsw_dev;

USE ddsw_dev;

CREATE EXTERNAL TABLE IF NOT EXISTS dealer_hierarchy_temp_data
(
  file_name string,
  gen_id string,
  dms_system string,
  dms_version string,
  transmit_ts string,
  orig_transmit_ts string,
  transmit_by string,
  record_status string,
  dealer_updt_ind string,
  store_number string,
  part_type string,
  store_name string,
  inventory_store_number string,
  cat_emrg_dealer_code string,
  cat_stock_dealer_code string,
  stock_replen_store_number string,
  stock_replen_lead_time string,
  lst_updt_ts string,
  orig_lst_updt_ts string,
  lst_updt_by_id string,
  file_ins_ts string,
  orig_file_ins_ts string,
  exp_stock_plan_lead_time string
)
PARTITIONED BY (dealer_code string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
STORED AS
  INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat'
  OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION '${hiveconf:hadoop.tmp.dir.uri}/dealer_hierarchy/temp_data'
TBLPROPERTIES ('serialization.null.format'='')
;
