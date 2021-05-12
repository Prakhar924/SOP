

spark.sql("""select  OPT_OUT_FLG, MDM_STATUS_CD, trim(MDM_PARTY_ID) MDM_PARTY_ID,OPT_OUT_DT,mdm_ins_dt,mdm_last_mod_dt,mdm_eff_end_dt from ALL_ALL_R_GBL_MDM.lnd_cdw_party_prof where mdm_party_id is not null
minus
select  value, status, customer_id, date_of_consent,mdm_ins_date, mdm_last_mod_date, mdm_eff_end_date from all_all_e_gbl_customer.idl_mdm_consent""").createOrReplaceTempView("idl_mdm_consent")

//below query using fetch insert and update records 

spark.sql("select distinct xref.* from ALL_ALL_R_GBL_MDM.lnd_cdw_party_prof xref left anti join idl_mdm_consent idl on trim(xref.mdm_party_id)=trim(idl.mdm_party_id) and xref.mdm_party_id is not null union all select distinct xref.* from ALL_ALL_R_GBL_MDM.lnd_cdw_party_prof xref join idl_mdm_consent idl on trim(xref.mdm_party_id)=trim(idl.mdm_party_id) ").createOrReplaceTempView("lnd_cdw_party_prof")

spark.sql("""insert into ALL_ALL_R_GBL_MDM.stg_mdm_consent select null,'OPT_OUT_FLG', OPT_OUT_FLG, MDM_STATUS_CD, trim(MDM_PARTY_ID) mdm_party_id, null,FALSE,OPT_OUT_DT, null,null,null,'OPT_OUT_FLG_EMAIL', mdm_ins_dt,mdm_last_mod_dt,mdm_eff_end_dt,current_timestamp,'10224' batch_id,'MDM',null,'Y' from lnd_cdw_party_prof where mdm_party_id is not null  union all select null,'OPT_OUT_FLG', OPT_OUT_FLG, MDM_STATUS_CD, trim(MDM_PARTY_ID) mdm_party_id, null,FALSE,OPT_OUT_DT, null,null,null,'OPT_OUT_FLG_EMAIL', mdm_ins_dt,mdm_last_mod_dt,mdm_eff_end_dt,current_timestamp,'10224' batch_id,'MDM','BAD RECORD:MDM PARTY ID NOT PRESENT','E' from lnd_cdw_party_prof where mdm_party_id is null  union all select null,'NO_MKT_FLG', NO_MKT_FLG, MDM_STATUS_CD, trim(MDM_PARTY_ID) mdm_party_id, null,FALSE,NO_MKT_START_DT, NO_MKT_END_DT, null,null,'NO_MKT_FLG', mdm_ins_dt,mdm_last_mod_dt,mdm_eff_end_dt,current_timestamp,'10224' batch_id,'MDM',null,'Y' from lnd_cdw_party_prof where mdm_party_id is not null  union all select null,'NO_MKT_FLG', NO_MKT_FLG, MDM_STATUS_CD, trim(MDM_PARTY_ID) mdm_party_id, null,FALSE,NO_MKT_START_DT, NO_MKT_END_DT, null,null,'NO_MKT_FLG', mdm_ins_dt,mdm_last_mod_dt,mdm_eff_end_dt,current_timestamp,'10224' batch_id,'MDM','BAD RECORD:MDM PARTY ID NOT PRESENT','E' from lnd_cdw_party_prof where mdm_party_id is null """)

lnd_cdw_party_prof