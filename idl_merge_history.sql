
spark.sql("""select prev_mdm_id,present_mdm_id,merge_date,rec_type  from all_all_r_gbl_mdm.lnd_vdw_merge_history
minus
select prev_mdm_id, present_mdm_id, merge_date, rec_type from all_all_e_gbl_customer.idl_merge_history""").createOrReplaceTempView("idl_merge_history")


//below query using fetch insert and update records  


spark.sql("select distinct xref.* from ALL_ALL_R_GBL_MDM.lnd_vdw_merge_history xref left anti join idl_merge_history idl on trim(xref.prev_mdm_id)=trim(idl.prev_mdm_id) union all select distinct xref.* from ALL_ALL_R_GBL_MDM.lnd_vdw_merge_history xref join idl_merge_history idl on trim(xref.prev_mdm_id)=trim(idl.prev_mdm_id)").createOrReplaceTempView("lnd_vdw_merge_history")

spark.sql("""insert into ALL_ALL_E_GBL_CUSTOMER.idl_merge_history select null odw_id,b.prev_mdm_id,b.present_mdm_id,b.merge_date,b.rec_type, b.batch_id batch_id_insert,b.batch_id batch_id_update,current_timestamp rec_insert_date, 'MDM' rec_insert_by,current_timestamp rec_modify_date,'MDM' rec_modify_by FROM (select trim(prev_mdm_id) prev_mdm_id ,trim(present_mdm_id) present_mdm_id FROM lnd_vdw_merge_history MINUS select trim(prev_mdm_id) prev_mdm_id ,trim(present_mdm_id) present_mdm_id FROM ALL_ALL_E_GBL_CUSTOMER.idl_merge_history) a ,lnd_vdw_merge_history b where a.prev_mdm_id = b.prev_mdm_id and a.present_mdm_id = b.present_mdm_id""")