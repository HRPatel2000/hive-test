SET mapreduce.job.queuename=ddsw;
SET hive.exec.dynamic.partition.mode=nonstrict;

USE ddsw_dev;

CREATE TABLE dealer_hierarchy_delta_prim AS
SELECT
  file_name,
  gen_id,
  dms_system,
  dms_version,
  transmit_ts,
  orig_transmit_ts,
  transmit_by,
  record_status,
  dealer_updt_ind,
  store_number,
  part_type,
  store_name,
  inventory_store_number,
  cat_emrg_dealer_code,
  cat_stock_dealer_code,
  stock_replen_store_number,
  stock_replen_lead_time,
  lst_updt_ts,
  orig_lst_updt_ts,
  lst_updt_by_id,
  file_ins_ts,
  orig_file_ins_ts,
  exp_stock_plan_lead_time,
  dealer_code
FROM dealer_hierarchy_delta;



