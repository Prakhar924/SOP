
spark.sql("""select trim(mdm_party_id) mdm_party_id,trim(mdm_party_id),first_name,middle_name,last_name, suffix,dob,gender,mdm_cust_type,full_name,mdm_cust_type,mdm_cust_subtype, mdm_ins_dt,mdm_last_mod_dt,mdm_status_cd,mdm_cust_subtype,mdm_eff_end_dt from ALL_ALL_R_GBL_MDM.LND_CDW_PARTY_PROF where mdm_party_id is not null
minus
select trim(hcp_id) hcp_id,trim(customer_id) customer_id, first_name, middle_name, last_name, suffix, date_of_birth, gender,customer_type, full_name,hcp_type, hcp_sub_type,mdm_ins_date, mdm_last_mod_date, mdm_status_code, mdm_cust_subtype,mdm_eff_end_date from all_all_e_gbl_customer.idl_mdm_contacts""").createOrReplaceTempView("idl_mdm_contacts")

//below query using fetch insert and update records 

spark.sql("select distinct xref.* from ALL_ALL_R_GBL_MDM.LND_CDW_PARTY_PROF xref left anti join idl_mdm_contacts idl on trim(xref.mdm_party_id)=trim(idl.mdm_party_id) and xref.mdm_party_id is not null union all select distinct xref.* from ALL_ALL_R_GBL_MDM.LND_CDW_PARTY_PROF xref  join idl_mdm_contacts idl on trim(xref.mdm_party_id)=trim(idl.mdm_party_id) ").createOrReplaceTempView("LND_CDW_PARTY_PROF")



spark.sql(""" insert into ALL_ALL_R_GBL_MDM.stg_mdm_contacts select mdm_party_id,mdm_party_id,null,first_name,middle_name,last_name, suffix,null,dob,gender,null,null,null,mdm_cust_type,null,null,full_name, null,null,null,mdm_cust_type,mdm_cust_subtype,mdm_cust_group_cd, mdm_ins_dt,mdm_last_mod_dt,mdm_eff_end_dt,mdm_status_cd,mdm_cust_subtype,current_timestamp,'8888' batch_id,'MDM',null,'Y' from LND_CDW_PARTY_PROF where mdm_party_id is not null    union all select mdm_party_id,mdm_party_id,null, first_name,middle_name,last_name, suffix,null,dob,gender,null,null,null,mdm_cust_type,null,null,full_name, null,null,null,mdm_cust_type, mdm_cust_subtype,mdm_cust_group_cd, mdm_ins_dt,mdm_last_mod_dt,mdm_eff_end_dt,mdm_status_cd,mdm_cust_subtype,current_timestamp, '8888' batch_id,'MDM','Rejected as mdm_party_id is null','E' from LND_CDW_PARTY_PROF where mdm_party_id is null  """) 

