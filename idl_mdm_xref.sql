spark.sql("""select trim(mdm_party_id) mdm_party_id ,mdm_src_sys_cd,mdm_cust_ssk  from all_all_r_gbl_mdm.lnd_cdw_party_xref where mdm_party_id is not null  minus
select trim(customer_id) customer_id, source_name, source_code  from all_all_e_gbl_customer.idl_mdm_xref""").createOrReplaceTempView("idl_mdm_xref")

//below query using fetch insert and update records 

spark.sql("select distinct xref.* from all_all_r_gbl_mdm.lnd_cdw_party_xref xref left anti join idl_mdm_xref idl on trim(xref.mdm_party_id)=trim(idl.mdm_party_id) and xref.mdm_party_id is not null union all select distinct xref.* from all_all_r_gbl_mdm.lnd_cdw_party_xref xref  join idl_mdm_xref idl on trim(xref.mdm_party_id)=trim(idl.mdm_party_id) ").createOrReplaceTempView("lnd_cdw_party_xref")


spark.sql("""select trim(mdm_party_id) mdm_party_id,trim(mdm_src_sys_cd) mdm_src_sys_cd,trim(mdm_cust_ssk) mdm_cust_ssk from  ALL_ALL_R_GBL_MDM.lnd_cdw_party_xref_prof where  mdm_party_id is not null 
minus
select trim(customer_id) customer_id, trim(source_name) source_name, trim(source_code) source_code from all_all_e_gbl_customer.idl_mdm_xref """).createOrReplaceTempView("idl_mdm_xref1")

//below query using fetch insert and update records 

spark.sql("select xref.* from all_all_r_gbl_mdm.lnd_cdw_party_xref_prof xref left anti join idl_mdm_xref1 idl on trim(xref.mdm_party_id)=trim(idl.mdm_party_id) and xref.mdm_party_id is not null union all select xref.* from all_all_r_gbl_mdm.lnd_cdw_party_xref_prof xref join idl_mdm_xref1 idl on trim(xref.mdm_party_id)=trim(idl.mdm_party_id)").createOrReplaceTempView("lnd_cdw_party_xref_prof")

//below query using fetch insert and update records 

spark.sql("""select trim(mdm_party_id) mdm_party_id,trim(mdm_attrib_type) mdm_attrib_type,trim(mdm_attrib_value)  mdm_attrib_value from  ALL_ALL_R_GBL_MDM.lnd_cdw_party_n_prof where mdm_attrib_type = 'IMS_ID' and mdm_party_id is not null
minus
select trim(customer_id)  customer_id, trim(source_name) source_name, trim(source_code) source_code from all_all_e_gbl_customer.idl_mdm_xref""").createOrReplaceTempView("idl_mdm_xref2") 

//below query using fetch insert and update records 

spark.sql("select xref.* from all_all_r_gbl_mdm.lnd_cdw_party_n_prof xref left anti join idl_mdm_xref2 idl on trim(xref.mdm_party_id)=trim(idl.mdm_party_id) and xref.mdm_party_id is not null union all select xref.* from all_all_r_gbl_mdm.lnd_cdw_party_n_prof xref join idl_mdm_xref2 idl on trim(xref.mdm_party_id)=trim(idl.mdm_party_id) ").createOrReplaceTempView("lnd_cdw_party_n_prof")


spark.sql("""insert into ALL_ALL_R_GBL_MDM.stg_mdm_xref select trim(mdm_party_id) mdm_party_id,mdm_src_sys_cd,mdm_cust_ssk,current_timestamp,src_eff_end_dt, mdm_ins_dt,mdm_last_mod_dt,null,current_timestamp,'BATCH_ID' as batch_id,'MDM', null,'Y',null from lnd_cdw_party_xref where mdm_party_id is not null  union all  select trim(mdm_party_id) mdm_party_id,mdm_src_sys_cd,mdm_cust_ssk,current_timestamp,src_eff_end_dt, mdm_ins_dt,mdm_last_mod_dt,null,current_timestamp,'BATCH_ID' as batch_id,'MDM', 'BAD RECORD:MDM PARTY ID NOT PRESENT','E',null from lnd_cdw_party_xref where mdm_party_id is null """) 

spark.sql("""insert into ALL_ALL_R_GBL_MDM.stg_mdm_xref   select trim(mdm_party_id) mdm_party_id,mdm_src_sys_cd,mdm_cust_ssk,current_timestamp,null, mdm_ins_dt,mdm_last_mod_dt,mdm_eff_end_dt,current_timestamp,'BATCH_ID' as batch_id,'MDM', null,'Y',mdm_status_cd from lnd_cdw_party_xref_prof where mdm_party_id is not null   union all select trim(mdm_party_id) mdm_party_id,mdm_src_sys_cd,mdm_cust_ssk,current_timestamp,null, mdm_ins_dt,mdm_last_mod_dt,mdm_eff_end_dt,current_timestamp,'BATCH_ID' as batch_id,'MDM', 'BAD RECORD:MDM PARTY ID NOT PRESENT','E',mdm_status_cd from lnd_cdw_party_xref_prof where mdm_party_id is null  """)

spark.sql("""insert into ALL_ALL_R_GBL_MDM.stg_mdm_xref  select trim(mdm_party_id) customer_id,mdm_attrib_type source_name,mdm_attrib_value source_code,null effective_date,null expiration_date, MDM_INS_DT mdm_ins_date,MDM_LAST_MOD_DT mdm_last_mod_date,MDM_EFF_END_DT mdm_eff_end_date,current_timestamp() rec_insert_date, 'BATCH_ID' as batch_id,'MDM' rec_insert_by,null reject_reason,'Y' status_flag,mdm_status_cd mdm_status_code from lnd_cdw_party_n_prof where mdm_attrib_type = 'IMS_ID' and mdm_party_id is not null   union all select trim(mdm_party_id) customer_id,mdm_attrib_type source_name,mdm_attrib_value source_code,null effective_date,null expiration_date, MDM_INS_DT mdm_ins_date,MDM_LAST_MOD_DT mdm_last_mod_date,MDM_EFF_END_DT mdm_eff_end_date,current_timestamp() rec_insert_date, 'BATCH_ID' as batch_id,'MDM' rec_insert_by,'BAD RECORD:MDM PARTY ID NOT PRESENT' reject_reason,'E' status_flag,mdm_status_cd mdm_status_code from lnd_cdw_party_n_prof where mdm_attrib_type = 'IMS_ID' and mdm_party_id is null """)