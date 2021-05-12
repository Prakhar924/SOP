-------------------------to take data backup-----------------------------------------------------------

spark.sql("""create table all_all_r_all_bkp.idl_mdm_consent_24_02 like all_all_e_gbl_customer.idl_mdm_consent""")

spark.sql("""insert overwrite table all_all_r_all_bkp.idl_mdm_consent_24_02 select * from all_all_e_gbl_customer.idl_mdm_consent""")

-------------------------to take data backup-----------------------------------------------------------

spark.sql("""insert overwrite table all_all_e_gbl_customer.idl_mdm_consent_temp select b.consent_id,b.type,b.value,b.status,b.customer_id,b.address_id,b.country,b.is_global,b.date_of_consent,b.issue_expiration_date,b.brand,b.product_id,b.consent_defintion,b.batch_id_insert,b.batch_id_update,b.rec_insert_by,b.rec_modify_by,b.rec_modify_date,b.rec_insert_date,b.mdm_ins_date,b.mdm_last_mod_date,b.mdm_eff_end_date from (select consent_id,type,value,status,customer_id,address_id,country,is_global,date_of_consent,issue_expiration_date,brand,product_id,consent_defintion,batch_id_insert,batch_id_update,rec_insert_by,rec_modify_by,rec_modify_date,rec_insert_date,mdm_ins_date,mdm_last_mod_date,mdm_eff_end_date, ROW_NUMBER() over (partition by type,value,status,country,customer_id,product_id,date_of_consent  order by rec_modify_date desc) RowNumber  from all_all_e_gbl_customer.idl_mdm_consent a ) b where RowNumber=1 """)

spark.sql("""insert overwrite table all_all_e_gbl_customer.idl_mdm_consent select distinct * from all_all_e_gbl_customer.idl_mdm_consent_temp""")
