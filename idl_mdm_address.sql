
spark.sql("""select trim(mdm_addr_id) mdm_addr_id,trim(mdm_party_id)  mdm_party_id,concat(addr_line_1,',',addr_line_2,',',addr_line_3,',',city,',',state,',',county,',',postal_cd_prim) ,addr_line_1,addr_line_2,addr_line_3,city,state,country,eff_end_dt, mdm_ins_dt,mdm_last_mod_dt,eff_end_dt from all_all_r_gbl_mdm.lnd_cdw_party_addr  where mdm_party_id is not null 
minus
select trim(addr_id) addr_id,trim(customer_id) customer_id, addr, addr_line_1, addr_line_2, addr_line_3, city, state,country,effective_date, mdm_ins_date, mdm_last_mod_date, mdm_eff_end_date from all_all_e_gbl_customer.idl_mdm_address""").createOrReplaceTempView("idl_mdm_address")

//below query using fetch insert and update records 

spark.sql("select distinct xref.* from all_all_r_gbl_mdm.lnd_cdw_party_addr xref left anti join idl_mdm_address idl on trim(xref.mdm_party_id)=trim(idl.mdm_party_id) and xref.mdm_party_id is not null union all select distinct xref.* from all_all_r_gbl_mdm.lnd_cdw_party_addr xref join idl_mdm_address idl on trim(xref.mdm_party_id)=trim(idl.mdm_party_id) ").createOrReplaceTempView("lnd_cdw_party_addr")



spark.sql("""select trim(mdm_addr_id)  mdm_addr_id,trim(mdm_party_id) mdm_party_id,concat(addr_line_1,',',addr_line_2,',',addr_line_3,',',city,',',state,',',county,',',postal_cd_prim),addr_line_1,addr_line_2,addr_line_3,city,state,country,mdm_eff_end_dt,mdm_ins_dt,mdm_last_mod_dt,mdm_eff_end_dt from  ALL_ALL_R_GBL_MDM .lnd_cdw_party_addr_prof where mdm_party_id is not null 
minus
select trim(addr_id),trim(customer_id), addr, addr_line_1, addr_line_2, addr_line_3, city, state,country,effective_date, mdm_ins_date, mdm_last_mod_date, mdm_eff_end_date from all_all_e_gbl_customer.idl_mdm_address""").createOrReplaceTempView("idl_mdm_address1")

//below query using fetch insert and update records 

spark.sql("select distinct xref.* from all_all_r_gbl_mdm.lnd_cdw_party_addr_prof xref left anti join idl_mdm_address1 idl on trim(xref.mdm_party_id)=trim(idl.mdm_party_id) and xref.mdm_party_id is not null union all select distinct xref.* from all_all_r_gbl_mdm.lnd_cdw_party_addr_prof xref join idl_mdm_address1 idl on trim(xref.mdm_party_id)=trim(idl.mdm_party_id)").createOrReplaceTempView("lnd_cdw_party_addr_prof")


spark.sql("""insert into  ALL_ALL_R_GBL_MDM .stg_mdm_address select trim(mdm_addr_id),trim(mdm_party_id) mdm_party_id,FALSE,'HCO',concat(addr_line_1,',',addr_line_2,',',addr_line_3,',',city,',',state,',',county,',',postal_cd_prim) ,addr_line_1,addr_line_2,addr_line_3,city,state,county, postal_cd_prim,country,null,null,null,eff_end_dt,trim(status_cd),trim(valid_addr_ind),' ','Y',postal_cd_sec, mdm_ins_dt,mdm_last_mod_dt,eff_end_dt,'7777' batch_id,current_timestamp,'MDM',null,'Y' from  lnd_cdw_party_addr where mdm_party_id is not null    union all  select trim(mdm_addr_id),trim(mdm_party_id) mdm_party_id,FALSE,'HCO',concat(addr_line_1,',',addr_line_2,',',addr_line_3,',',city,',',state,',',county,',',postal_cd_prim),addr_line_1,addr_line_2,addr_line_3,city,state,county, postal_cd_prim,country,null,null,null,eff_end_dt,trim(status_cd),trim(valid_addr_ind),' ','Y',postal_cd_sec, mdm_ins_dt,mdm_last_mod_dt,eff_end_dt,'7777' batch_id,current_timestamp,'MDM','BAD RECORD:MDM PARTY ID NOT PRESENT','E' from  lnd_cdw_party_addr where mdm_party_id is null  """)

spark.sql("""insert into  ALL_ALL_R_GBL_MDM .stg_mdm_address select trim(mdm_addr_id),trim(mdm_party_id) mdm_party_id,FALSE,'HCP', concat(addr_line_1,',',addr_line_2,',',addr_line_3,',',city,',',state,',',county,',',postal_cd_prim),addr_line_1,addr_line_2,addr_line_3,city,state,county, postal_cd_prim,country,null,null,null,mdm_eff_end_dt,trim(mdm_status_cd),trim(valid_addr_ind),trim(dmr_addr_status),'N',postal_cd_sec, mdm_ins_dt,mdm_last_mod_dt,mdm_eff_end_dt,'7777' batch_id,current_timestamp,'MDM',null,'Y' from  lnd_cdw_party_addr_prof where mdm_party_id is not null   union all select trim(mdm_addr_id),trim(mdm_party_id) mdm_party_id,FALSE,'HCP', concat(addr_line_1,',',addr_line_2,',',addr_line_3,',',city,',',state,',',county,',',postal_cd_prim),addr_line_1,addr_line_2,addr_line_3,city,state,county, postal_cd_prim,country,null,null,null,mdm_eff_end_dt,trim(mdm_status_cd),trim(valid_addr_ind),trim(dmr_addr_status),'N',postal_cd_sec, mdm_ins_dt,mdm_last_mod_dt,mdm_eff_end_dt,'7777' batch_id,current_timestamp,'MDM','BAD RECORD:MDM PARTY ID NOT PRESENT','E' from  lnd_cdw_party_addr_prof where mdm_party_id is null  """)
