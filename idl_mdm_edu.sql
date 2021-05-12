
spark.sql("""select null education_id,mdm_attrib_value,mdm_attrib_value_2,mdm_party_id,mdm_status_cd,mdm_ins_dt,mdm_last_mod_dt,mdm_eff_end_dt  from ALL_ALL_R_GBL_MDM.lnd_cdw_party_n_prof prof where upper(mdm_attrib_type) = 'DEGREE' and mdm_attrib_value_2 is not null and mdm_attrib_value_2!='null' and mdm_party_id is not null
minus
select education_id, degree_title, degree,customer_id, status,mdm_ins_date,mdm_last_mod_date, mdm_eff_end_date from all_all_e_gbl_customer.idl_mdm_edu
""").createOrReplaceTempView("idl_mdm_edu")

//below query using fetch insert and update records 

spark.sql("select distinct xref.* from ALL_ALL_R_GBL_MDM.lnd_cdw_party_n_prof xref left anti join idl_mdm_edu idl on trim(xref.mdm_party_id)=trim(idl.mdm_party_id) and   upper(xref.mdm_attrib_type) = 'DEGREE' and xref.mdm_attrib_value_2 is not null and xref.mdm_attrib_value_2!='null' and xref.mdm_party_id is not null union all select distinct xref.* from ALL_ALL_R_GBL_MDM.lnd_cdw_party_n_prof xref join idl_mdm_edu idl on trim(xref.mdm_party_id)=trim(idl.mdm_party_id)").createOrReplaceTempView("lnd_cdw_party_n_prof")

spark.sql("""insert into ALL_ALL_R_GBL_MDM.stg_mdm_edu select null,mdm_attrib_value,mdm_attrib_value_2,null,null,mdm_party_id,mdm_status_cd,mdm_ins_dt,mdm_last_mod_dt,mdm_eff_end_dt,current_timestamp,batch_id,'MDM','','Y' from lnd_cdw_party_n_prof prof where upper(mdm_attrib_type) = 'DEGREE' and mdm_attrib_value_2 is not null and mdm_attrib_value_2!='null' and mdm_party_id is not null     union all select null,mdm_attrib_value,mdm_attrib_value_2,null,null,mdm_party_id,mdm_status_cd,mdm_ins_dt,mdm_last_mod_dt,mdm_eff_end_dt,current_timestamp,batch_id,'MDM','BAD RECORD: NO MDM PARTY ID','E' from lnd_cdw_party_n_prof prof where upper(mdm_attrib_type) = 'DEGREE' and mdm_attrib_value_2 is not null and mdm_attrib_value_2!='null' and mdm_party_id is null """)