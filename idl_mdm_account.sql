// doing minus and fetching records from source table 

spark.sql("""select trim(mdm_party_id) mdm_party_id,trim(mdm_party_id),org_name,alt_org_name,tot_census_beds,full_name,mdm_cust_type,mdm_cust_subtype,tot_lic_beds,tot_staffed_beds,tot_or_surg,tot_surg,tot_procs,mii_org_fac_spec,mii_org_fac_type,mii_org_cot,nov_cot,alt_org_name,full_name,mdm_ins_dt,mdm_cust_stat_cd from all_all_r_gbl_mdm.lnd_cdw_party where mdm_party_id is not null minus select trim(hco_id),trim(customer_id), name, alias, number_of_beds, hco_legal_entity, hco_type, hco_sub_type, tot_lic_beds, tot_staffed_beds, tot_or_surg, tot_surg, tot_procs, mii_org_fac_spec, mii_org_fac_type, mii_org_cot, nov_cot, alt_org_name, full_name,mdm_ins_date, mdm_status_code from all_all_e_gbl_customer.idl_mdm_account""").createOrReplaceTempView("idl_mdm_account")

//below query using fetch insert and update records 

spark.sql("select distinct xref.* from all_all_r_gbl_mdm.lnd_cdw_party xref left anti join idl_mdm_account idl on trim(xref.mdm_party_id)=trim(idl.mdm_party_id) and xref.mdm_party_id is not null union all select distinct xref.* from all_all_r_gbl_mdm.lnd_cdw_party xref join idl_mdm_account idl on trim(xref.mdm_party_id)=trim(idl.mdm_party_id) ").createOrReplaceTempView("lnd_cdw_party ")



spark.sql("""insert into ALL_ALL_R_GBL_MDM.stg_mdm_account select MDM_PARTY_ID,MDM_PARTY_ID,ORG_NAME,ALT_ORG_NAME,null,TOT_CENSUS_BEDS,null,FULL_NAME,null,null,null,null,null,null,null,null,MDM_CUST_TYPE,MDM_CUST_SUBTYPE,TOT_LIC_BEDS,TOT_STAFFED_BEDS,TOT_OR_SURG,TOT_SURG,TOT_PROCS,MII_ORG_FAC_SPEC,MII_ORG_FAC_TYPE,MII_ORG_COT,NOV_COT,ALT_ORG_NAME,FULL_NAME,MDM_CUST_SUBTYPE,MDM_CUST_TYPE,null,MDM_LAST_MOD_DT,null,MMA_ACCT_FLG,MDM_INS_DT,MDM_CUST_STAT_CD,batch_id,current_timestamp,'MDM',null,'Y' from lnd_cdw_party where MDM_PARTY_ID is not null  union all select MDM_PARTY_ID,MDM_PARTY_ID,ORG_NAME,ALT_ORG_NAME,null,TOT_CENSUS_BEDS,null,FULL_NAME,null,null,null,null,null,null,null,null,MDM_CUST_TYPE,MDM_CUST_SUBTYPE,TOT_LIC_BEDS,TOT_STAFFED_BEDS,TOT_OR_SURG,TOT_SURG,TOT_PROCS,MII_ORG_FAC_SPEC,MII_ORG_FAC_TYPE,MII_ORG_COT,NOV_COT,ALT_ORG_NAME,FULL_NAME,MDM_CUST_SUBTYPE,MDM_CUST_TYPE,null,MDM_LAST_MOD_DT,null,MMA_ACCT_FLG,MDM_INS_DT,MDM_CUST_STAT_CD,batch_id,current_timestamp,'MDM','BAD RECORD: NO MDM ID','E' from lnd_cdw_party where MDM_PARTY_ID is null """)


