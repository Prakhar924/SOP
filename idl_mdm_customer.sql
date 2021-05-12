
spark.sql("""select trim(stg.mdm_party_id) mdm_party_id, trim(stg.mdm_party_id), trim(stg.mdm_cust_stat_cd),trim(stg.mdm_cust_type),trim(stg.mdm_cust_subtype) from all_all_r_gbl_mdm.lnd_cdw_party stg  where mdm_party_id is not null 
minus 
select trim(customer_id) , trim(hco_id),trim(status),trim(type),trim(sub_type)  from all_all_e_gbl_customer.idl_mdm_customer""").createOrReplaceTempView("idl_mdm_customer")

--select trim(stg.mdm_party_id),null hco_id,trim(stg.mdm_status_cd),trim(stg.mdm_cust_type),trim(stg.mdm_cust_subtype) from ALL_ALL_R_GBL_MDM.lnd_cdw_party_prof stg where mdm_party_id is not null  minus   select trim(customer_id) , trim(hco_id),trim(status),trim(type),trim(sub_type)  from all_all_e_gbl_customer.idl_mdm_customer  --0 records

//below query using fetch insert and update records 

spark.sql("select distinct xref.* from all_all_r_gbl_mdm.lnd_cdw_party xref left anti join idl_mdm_customer idl on trim(xref.mdm_party_id)=trim(idl.mdm_party_id) and xref.mdm_party_id is not null union all select distinct xref.* from all_all_r_gbl_mdm.lnd_cdw_party xref  join idl_mdm_customer idl on trim(xref.mdm_party_id)=trim(idl.mdm_party_id) ").createOrReplaceTempView("lnd_cdw_party")


spark.sql("""insert into ALL_ALL_R_GBL_MDM.stg_mdm_customer select trim(lnd.mdm_party_id) as customer_id , trim(lnd.mdm_party_id) as hco_id , null as hcp_id , mdm_cust_stat_cd as status , null as status_reason , lnd.mdm_cust_type as type , lnd.mdm_cust_subtype as sub_type , 'N' as health_care_professional , 'Y' as health_care_organization , lnd.mdm_ins_dt as mdm_ins_date,lnd.mdm_last_mod_dt as mdm_last_mod_date,null as mdm_eff_end_date,'5555' batch_id,current_timestamp rec_insert_date,'MDM' as rec_insert_by,'Y' status_flag,null reject_reason from  lnd_cdw_party lnd where mdm_party_id is not null """)

