------------------- From Gayathri ----------------------

----------- New set ----------------

----idl_mdm_credentials------

spark.sql("""create table all_all_e_gbl_customer_qa3.idl_mdm_credentials_1412 like all_all_e_gbl_customer_qa3.idl_mdm_credentials""");

spark.sql("""insert into all_all_e_gbl_customer_qa3.idl_mdm_credentials_1412 select * from all_all_e_gbl_customer_qa3.idl_mdm_credentials""");

spark.sql(""" drop table all_all_e_gbl_customer_qa3.idl_mdm_credentials_temp""");

spark.sql(""" create table all_all_e_gbl_customer_qa3.idl_mdm_credentials_temp like all_all_e_gbl_customer_qa3.idl_mdm_credentials""");

spark.sql("""truncate table all_all_e_gbl_customer_qa3.idl_mdm_credentials_temp""");

spark.sql("""insert into all_all_e_gbl_customer_qa3.idl_mdm_credentials_temp 
select b.credential_id,b.type,b.value,b.status,b.issue_state,b.issue_country,b.issue_date,b.issue_expiration_date,b.customer_id,b.batch_id_insert,b.batch_id_update,b.rec_insert_by,b.rec_modify_by,b.rec_modify_date,b.rec_insert_date,b.mdm_ins_date,b.mdm_last_mod_date,b.mdm_eff_end_date,b.controlflag
from (select credential_id,type,value,status,issue_state,issue_country,issue_date,issue_expiration_date,customer_id,batch_id_insert,batch_id_update,rec_insert_by,rec_modify_by,rec_modify_date,rec_insert_date,mdm_ins_date,mdm_last_mod_date,mdm_eff_end_date,controlflag,
ROW_NUMBER() over (partition by customer_id,type,value,issue_state order by rec_modify_date desc) RowNumber from all_all_e_gbl_customer_qa3.idl_mdm_credentials a ) b where RowNumber=1""");

spark.sql("""select * from all_all_e_gbl_customer_qa3.idl_mdm_credentials_temp""").count()
--31798491
spark.sql("""select * from all_all_e_gbl_customer_qa3.idl_mdm_credentials""").count()
--31800800

spark.sql("""truncate table all_all_e_gbl_customer_qa3.idl_mdm_credentials""");

spark.sql("""insert into all_all_e_gbl_customer_qa3.idl_mdm_credentials select distinct * from all_all_e_gbl_customer_qa3.idl_mdm_credentials_temp""");

==========================================================

---idl_mdm_edu------

spark.sql("""create table all_all_e_gbl_customer_qa3.idl_mdm_edu_1412 like all_all_e_gbl_customer_qa3.idl_mdm_edu""");

spark.sql("""insert into all_all_e_gbl_customer_qa3.idl_mdm_edu_1412 select * from all_all_e_gbl_customer_qa3.idl_mdm_edu""");

spark.sql(""" drop table all_all_e_gbl_customer_qa3.idl_mdm_edu_temp""");

spark.sql(""" create table all_all_e_gbl_customer_qa3.idl_mdm_edu_temp like all_all_e_gbl_customer_qa3.idl_mdm_edu""");

spark.sql("""truncate table all_all_e_gbl_customer_qa3.idl_mdm_edu_temp""");

spark.sql("""insert into all_all_e_gbl_customer_qa3.idl_mdm_edu_temp select b.education_id,b.degree_title,b.degree,b.is_primary,b.graduation_year,b.institution,b.customer_id,b.status,b.batch_id_insert,b.batch_id_update,b.rec_insert_by,b.rec_modify_by,b.rec_modify_date,b.rec_insert_date,b.mdm_last_mod_date,b.mdm_eff_end_date,b.mdm_ins_date
from (select education_id,degree_title,degree,is_primary,graduation_year,institution,customer_id,status,batch_id_insert,batch_id_update,rec_insert_by,rec_modify_by,rec_modify_date,rec_insert_date,mdm_last_mod_date,mdm_eff_end_date,mdm_ins_date,rank() over (partition by customer_id order by rec_modify_date desc) rnk from all_all_e_gbl_customer_qa3.idl_mdm_edu) b where rnk=1""");

spark.sql("""select * from all_all_e_gbl_customer_qa3.idl_mdm_edu_temp""").count()
--9231635
spark.sql("""select * from all_all_e_gbl_customer_qa3.idl_mdm_edu""").count()
--9231635

spark.sql("""truncate table all_all_e_gbl_customer_qa3.idl_mdm_edu""");

spark.sql("""insert into all_all_e_gbl_customer_qa3.idl_mdm_edu select * from all_all_e_gbl_customer_qa3.idl_mdm_edu_temp""");


===========================================================

----idl_mdm_consent-----

spark.sql("""create table all_all_e_gbl_customer_qa3.idl_mdm_consent_1412 like all_all_e_gbl_customer_qa3.idl_mdm_consent""");

spark.sql("""insert into all_all_e_gbl_customer_qa3.idl_mdm_consent_1412 select * from all_all_e_gbl_customer_qa3.idl_mdm_consent""");

spark.sql(""" drop table all_all_e_gbl_customer_qa3.idl_mdm_consent_temp""");

spark.sql(""" create table all_all_e_gbl_customer_qa3.idl_mdm_consent_temp like all_all_e_gbl_customer_qa3.idl_mdm_consent""");

spark.sql("""truncate table all_all_e_gbl_customer_qa3.idl_mdm_consent_temp""");

spark.sql("""insert into all_all_e_gbl_customer_qa3.idl_mdm_consent_temp 
select b.consent_id,b.type,b.value,b.status,b.customer_id,b.address_id,b.country,b.is_global,b.date_of_consent,b.issue_expiration_date,b.brand,b.product_id,b.consent_defintion,b.batch_id_insert,b.batch_id_update,b.rec_insert_by,b.rec_modify_by,b.rec_modify_date,b.rec_insert_date,b.mdm_ins_date,b.mdm_last_mod_date,b.mdm_eff_end_date from (select consent_id,type,value,status,customer_id,address_id,country,is_global,date_of_consent,issue_expiration_date,brand,product_id,consent_defintion,batch_id_insert,batch_id_update,rec_insert_by,rec_modify_by,rec_modify_date,rec_insert_date,mdm_ins_date,mdm_last_mod_date,mdm_eff_end_date,
ROW_NUMBER() over (partition by type,value,status,country,customer_id,product_id  order by rec_modify_date desc) RowNumber  from all_all_e_gbl_customer.idl_mdm_consent a ) b where RowNumber=1 """);

spark.sql("""select * from all_all_e_gbl_customer_qa3.idl_mdm_consent_temp""").count()
--18660705
spark.sql("""select * from all_all_e_gbl_customer_qa3.idl_mdm_consent""").count()
--19278906

spark.sql("""truncate table all_all_e_gbl_customer_qa3.idl_mdm_consent""");

spark.sql("""insert into all_all_e_gbl_customer_qa3.idl_mdm_consent select distinct * from all_all_e_gbl_customer_qa3.idl_mdm_consent_temp""");



------------------------------------

---idl_mdm_address----

spark.sql("""truncate table all_all_e_gbl_customer_qa3.idl_mdm_address_temp""")

spark.sql("""insert into all_all_e_gbl_customer_qa3.idl_mdm_address_temp 
select b.addr_id,b.customer_id,b.is_primary,b.addr_type,b.addr,b.addr_line_1,b.addr_line_2,b.addr_line_3,b.city,b.state,b.province,b.zip,b.country,b.msa,b.cbsa,b.covered_lives,b.brick,b.microbrick,b.geographic_coordinates,b.effective_date,b.addr_status,b.verification_status,b.status_reason,b.hco,b.batch_id_insert,b.batch_id_update,b.rec_insert_by,b.rec_modify_by,b.rec_modify_date,b.rec_insert_date,b.zip_ext,b.mdm_ins_date,b.mdm_last_mod_date,b.mdm_eff_end_date from (select addr_id,customer_id,is_primary,addr_type,addr,addr_line_1,addr_line_2,addr_line_3,city,state,province,zip,country,msa,cbsa,covered_lives,brick,microbrick,geographic_coordinates,effective_date,addr_status,verification_status,status_reason,hco,batch_id_insert,batch_id_update,rec_insert_by,rec_modify_by,rec_modify_date,rec_insert_date,zip_ext,mdm_ins_date,mdm_last_mod_date,mdm_eff_end_date,ROW_NUMBER() over (partition by customer_id,addr_id,addr_status  order by rec_modify_date desc) RowNumber from all_all_e_gbl_customer_qa3.idl_mdm_address a ) b where RowNumber=1 """);

spark.sql("""select * from all_all_e_gbl_customer_qa3.idl_mdm_address_temp""").count()
--33105323
spark.sql("""select * from all_all_e_gbl_customer_qa3.idl_mdm_address""").count()
--33105323
spark.sql("""truncate table all_all_e_gbl_customer_qa3.idl_mdm_address""");

spark.sql("""insert into all_all_e_gbl_customer_qa3.idl_mdm_address select distinct * from all_all_e_gbl_customer_qa3.idl_mdm_address_temp""");
==================================================== 
----idl_mdm_credentials---failed --- target table has 20 column(s) but the inserted data has 19 column(s)

spark.sql("""truncate table all_all_e_gbl_customer_qa3.idl_mdm_credentials_temp""");

spark.sql("""insert into all_all_e_gbl_customer_qa3.idl_mdm_credentials_temp 
select b.credential_id,b.type,b.value,b.status,b.issue_state,b.issue_country,b.issue_date,b.issue_expiration_date,b.customer_id,b.batch_id_insert,b.batch_id_update,b.rec_insert_by,b.rec_modify_by,b.rec_modify_date,b.rec_insert_date,b.mdm_ins_date,b.mdm_last_mod_date,b.mdm_eff_end_date,b.controlflag
from (select credential_id,type,value,status,issue_state,issue_country,issue_date,issue_expiration_date,customer_id,batch_id_insert,batch_id_update,rec_insert_by,rec_modify_by,rec_modify_date,rec_insert_date,mdm_ins_date,mdm_last_mod_date,mdm_eff_end_date,controlflag,
ROW_NUMBER() over (partition by customer_id,type,value,issue_state order by rec_modify_date desc) RowNumber from all_all_e_gbl_customer_qa3.idl_mdm_credentials a ) b where RowNumber=1""");

spark.sql("""truncate table all_all_e_gbl_customer_qa3.idl_mdm_credentials""");

spark.sql("""insert into all_all_e_gbl_customer_qa3.idl_mdm_credentials select distinct * from all_all_e_gbl_customer_qa3.idl_mdm_credentials_temp""");
===================================================
----idl_mdm_account-----

spark.sql("""truncate table all_all_e_gbl_customer_qa3.idl_mdm_account_temp""");

spark.sql("""insert into all_all_e_gbl_customer_qa3.idl_mdm_account_temp 
select b.hco_id,b.customer_id,b.name,b.alias,b.department,b.number_of_beds,b.theuraptic_area,b.hco_legal_entity,b.required_certifications,b.groupings,b.customer_type,b.affiliation,b.parent_primary,b.primary_addr_line_1,b.primary_zip,b.primary_country,b.hco_type,b.hco_sub_type,b.tot_lic_beds,b.tot_staffed_beds,b.tot_or_surg,b.tot_surg,b.tot_procs,b.mii_org_fac_spec,b.mii_org_fac_type,b.mii_org_cot,b.nov_cot,b.alt_org_name,b.full_name,b.tot_census_beds,b.electronic_med_rec,b.res_cnt,b.res_prog_flg,b.teach_hosp_flg,b.batch_id_insert,b.batch_id_update,b.rec_insert_by,b.rec_modify_by,b.rec_modify_date,b.rec_insert_date,b.mdm_ins_date,b.mdm_status_code,b.mdm_cust_subtype,b.mdm_cust_type,b.mdm_last_mod_date,b.mdm_cust_group_code,b.mma_acct_flag,b.mdm_eff_end_date from (select hco_id,customer_id,name,alias,department,number_of_beds,theuraptic_area,hco_legal_entity,required_certifications,groupings,customer_type,affiliation,parent_primary,primary_addr_line_1,primary_zip,primary_country,hco_type,hco_sub_type,tot_lic_beds,tot_staffed_beds,tot_or_surg,tot_surg,tot_procs,mii_org_fac_spec,mii_org_fac_type,mii_org_cot,nov_cot,alt_org_name,full_name,tot_census_beds,electronic_med_rec,res_cnt,res_prog_flg,teach_hosp_flg,batch_id_insert,batch_id_update,rec_insert_by,rec_modify_by,rec_modify_date,rec_insert_date,mdm_ins_date,mdm_status_code,mdm_cust_subtype,mdm_cust_type,mdm_last_mod_date,mdm_cust_group_code,mma_acct_flag,mdm_eff_end_date,ROW_NUMBER() over (partition by hco_id order by rec_modify_date desc) RowNumber  from all_all_e_gbl_customer_qa3.idl_mdm_account a ) b where RowNumber=1 """);

spark.sql("""select * from all_all_e_gbl_customer_qa3.idl_mdm_account_temp""").count()
--4362086
spark.sql("""select * from all_all_e_gbl_customer_qa3.idl_mdm_account""").count()
--4362184

spark.sql("""truncate table all_all_e_gbl_customer_qa3.idl_mdm_account""");

spark.sql("""insert into all_all_e_gbl_customer_qa3.idl_mdm_account select distinct * from all_all_e_gbl_customer_qa3.idl_mdm_account_temp""");

========================================================
---idl_mdm_edu----failed --
----org.apache.spark.sql.AnalysisException: cannot resolve '`b.education_id`' given input columns: [a.mdm_ins_date, a.customer_id, a.batch_id_update, a.rec_modify_by, a.rec_insert_by, a.batch_id_insert, a.degree_title, a.rec_insert_date, a.education_id, a.is_primary, a.rec_modify_date, a.status, a.rnk, a.mdm_eff_end_date, a.mdm_last_mod_date, a.graduation_year, a.institution, a.degree]; line 1 pos 63; 'InsertIntoTable 'UnresolvedRelation `all_all_e_gbl_customer_qa3`.`idl_mdm_edu_temp`, false, false +- 'Project ['b.education_id, 'b.degree_title, 'b.degree, 'b.is_primary, 'b.graduation_year, 'b.institution, 'b.customer_id, 'b.status, 'b.batch_id_insert, 'b.batch_id_update, 'b.rec_insert_by, 'b.rec_modify_by, 'b.rec_modify_date, 'b.rec_insert_date, 'b.mdm_last_mod_date, 'b.mdm_eff_end_date, 'b.mdm_ins_date]


spark.sql("""truncate table all_all_e_gbl_customer_qa3.idl_mdm_edu_temp""");

spark.sql("""insert into all_all_e_gbl_customer_qa3.idl_mdm_edu_temp select b.education_id,b.degree_title,b.degree,b.is_primary,b.graduation_year,b.institution,b.customer_id,b.status,b.batch_id_insert,b.batch_id_update,b.rec_insert_by,b.rec_modify_by,b.rec_modify_date,b.rec_insert_date,b.mdm_last_mod_date,b.mdm_eff_end_date,b.mdm_ins_date
from (select education_id,degree_title,degree,is_primary,graduation_year,institution,customer_id,status,batch_id_insert,batch_id_update,rec_insert_by,rec_modify_by,rec_modify_date,rec_insert_date,mdm_last_mod_date,mdm_eff_end_date,mdm_ins_date,rank() over (partition by customer_id order by rec_modify_date desc) rnk from all_all_e_gbl_customer_qa3.idl_mdm_edu) a where rnk=1""");

spark.sql("""select * from all_all_e_gbl_customer_qa3.idl_mdm_edu_temp""").count()
--
spark.sql("""select * from all_all_e_gbl_customer_qa3.idl_mdm_edu""").count()
--

spark.sql("""truncate table all_all_e_gbl_customer_qa3.idl_mdm_edu""");

spark.sql("""insert into all_all_e_gbl_customer_qa3.idl_mdm_edu select * from all_all_e_gbl_customer_qa3.idl_mdm_edu_temp""");

=======================================================

----idl_mdm_spclty_cust----

spark.sql("""truncate table all_all_e_gbl_customer_qa3.idl_mdm_spclty_cust_temp""");

spark.sql("""insert into all_all_e_gbl_customer_qa3.idl_mdm_spclty_cust_temp 
select b.specialty_id,b.type,b.value,b.status,b.customer_id,b.mdm_ins_date,b.mdm_last_mod_date,b.mdm_eff_end_date,b.batch_id_insert,b.batch_id_update,b.rec_insert_by,b.rec_modify_by,b.rec_modify_date,b.rec_insert_date from (select specialty_id,type,value,status,customer_id,mdm_ins_date,mdm_last_mod_date,mdm_eff_end_date,batch_id_insert,batch_id_update,rec_insert_by,rec_modify_by,rec_modify_date,rec_insert_date,ROW_NUMBER() over (partition by TYPE,VALUE,CUSTOMER_ID order by rec_modify_date desc) RowNumber  from all_all_e_gbl_customer_qa3.idl_mdm_spclty_cust a ) b where RowNumber=1 """);

spark.sql("""select * from all_all_e_gbl_customer_qa3.idl_mdm_spclty_cust_temp""").count()
--16848117
spark.sql("""select * from all_all_e_gbl_customer_qa3.idl_mdm_spclty_cust""").count()
--16848250

spark.sql("""truncate table all_all_e_gbl_customer_qa3.idl_mdm_spclty_cust""");

spark.sql("""insert into all_all_e_gbl_customer_qa3.idl_mdm_spclty_cust select distinct * from all_all_e_gbl_customer_qa3.idl_mdm_spclty_cust_temp""");
=========================================================
----idl_mdm_xref----


spark.sql("""truncate table all_all_e_gbl_customer_qa3.idl_mdm_xref_temp""");

spark.sql("""insert into all_all_e_gbl_customer_qa3.idl_mdm_xref_temp 
select b.customer_id,b.source_name,b.source_code,b.effective_date,b.expiration_date,b.batch_id_insert,b.batch_id_update,b.rec_insert_by,b.rec_modify_by,b.rec_modify_date,b.rec_insert_date,b.mdm_ins_date,b.mdm_last_mod_date,b.mdm_eff_end_date,b.mdm_status_code
from (select customer_id,source_name,source_code,effective_date,expiration_date,batch_id_insert,batch_id_update,rec_insert_by,rec_modify_by,rec_modify_date,rec_insert_date,mdm_ins_date,mdm_last_mod_date,mdm_eff_end_date,mdm_status_code,ROW_NUMBER() over (partition by CUSTOMER_ID,SOURCE_NAME,SOURCE_CODE  order by rec_modify_date desc) RowNumber  from all_all_e_gbl_customer_qa3.idl_mdm_xref a ) b where RowNumber=1 """);

spark.sql("""select * from all_all_e_gbl_customer_qa3.idl_mdm_xref_temp""").count()
--34652239
spark.sql("""select * from all_all_e_gbl_customer_qa3.idl_mdm_xref""").count()
--34799194

spark.sql("""truncate table all_all_e_gbl_customer_qa3.idl_mdm_xref""");

spark.sql("""insert into all_all_e_gbl_customer_qa3.idl_mdm_xref select distinct * from all_all_e_gbl_customer_qa3.idl_mdm_xref_temp"""); 

===========================================================





spark.sql("""truncate table all_all_e_gbl_customer_qa3.idl_mdm_edu_temp""")

spark.sql("""insert into all_all_e_gbl_customer_qa3.idl_mdm_edu_temp select education_id,degree_title,degree,graduation_year,institution,customer_id,status,batch_id_insert,batch_id_update,rec_insert_by,rec_modify_by,rec_modify_date,rec_insert_date,mdm_last_mod_date,mdm_eff_end_date,mdm_ins_date from (select education_id,degree_title,degree,graduation_year,institution,customer_id,status,batch_id_insert,batch_id_update,rec_insert_by,rec_modify_by,rec_modify_date,rec_insert_date,mdm_last_mod_date,mdm_eff_end_date,mdm_ins_date,rank() over (partition by customer_id order by rec_modify_date desc) rnk from all_all_e_gbl_customer_qa3.idl_mdm_edu) a where rnk=1""")

spark.sql("""select * from all_all_e_gbl_customer_qa3.idl_mdm_edu_temp""").count()

spark.sql("""select * from all_all_e_gbl_customer_qa3.idl_mdm_edu""").count()

spark.sql("""truncate table all_all_e_gbl_customer_qa3.idl_mdm_edu""")

spark.sql("""insert into all_all_e_gbl_customer_qa3.idl_mdm_edu select * from all_all_e_gbl_customer_qa3.idl_mdm_edu_temp""")

===========================================================================================================================================================

spark.sql("""truncate table all_all_e_gbl_customer_qa3.idl_mdm_address_temp""")
 
spark.sql("""insert into  all_all_e_gbl_customer_qa3.idl_mdm_address_temp  select addr_id	,customer_id	,is_primary	,addr_type	,addr	,addr_line_1	,addr_line_2	,addr_line_3	,city	,state	,province	,zip	,country	,msa	,cbsa	,covered_lives	,brick	,microbrick	,geographic_coordinates	,effective_date	,addr_status	,verification_status	,status_reason	,hco	,batch_id_insert	,batch_id_update	,rec_insert_by	,rec_modify_by	,rec_modify_date	,rec_insert_date	,zip_ext	,mdm_ins_date	,mdm_last_mod_date	,mdm_eff_end_date from (select addr_id	,customer_id	,is_primary	,addr_type	,addr	,addr_line_1	,addr_line_2	,addr_line_3	,city	,state	,province	,zip	,country	,msa	,cbsa	,covered_lives	,brick	,microbrick	,geographic_coordinates	,effective_date	,addr_status	,verification_status	,status_reason	,hco	,batch_id_insert	,batch_id_update	,rec_insert_by	,rec_modify_by	,rec_modify_date	,rec_insert_date	,zip_ext	,mdm_ins_date	,mdm_last_mod_date	,mdm_eff_end_date ,ROW_NUMBER() over (partition by customer_id,addr_id,addr_status order by rec_modify_date desc) rnk from all_all_e_gbl_customer_qa3.idl_mdm_address a) a where rnk=1""")
 
spark.sql("""truncate table all_all_e_gbl_customer_qa3.idl_mdm_address""")
  
  
spark.sql("""insert into all_all_e_gbl_customer_qa3.idl_mdm_address select * from all_all_e_gbl_customer_qa3.idl_mdm_address_temp""")


===========================================================================================================================================================


spark.sql("""truncate table all_all_e_gbl_customer_qa3.idl_mdm_credentials_temp""")
 
spark.sql("""insert into  all_all_e_gbl_customer_qa3.idl_mdm_credentials_temp  select credential_id,type,value,status,issue_state,issue_country,issue_date,issue_expiration_date,customer_id,batch_id_insert,batch_id_update,rec_insert_by,rec_modify_by,rec_modify_date,rec_insert_date,mdm_ins_date,mdm_last_mod_date,mdm_eff_end_date,controlflag from (select credential_id,type,value,status,issue_state,issue_country,issue_date,issue_expiration_date,customer_id,batch_id_insert,batch_id_update,rec_insert_by,rec_modify_by,rec_modify_date,rec_insert_date,mdm_ins_date,mdm_last_mod_date,mdm_eff_end_date,controlflag ,ROW_NUMBER() over (partition by customer_id,type,value,issue_state order by rec_modify_date desc) rnk from all_all_e_gbl_customer_qa3.idl_mdm_credentials a) a where rnk=1""")
 
spark.sql("""truncate table all_all_e_gbl_customer_qa3.idl_mdm_credentials""")
  
  
spark.sql("""insert into all_all_e_gbl_customer_qa3.idl_mdm_credentials select * from all_all_e_gbl_customer_qa3.idl_mdm_credentials_temp""")


===========================================================================================================================================================


spark.sql("""truncate table all_all_b_usa_crmods_qa3.ODS_CPIC_HCP_CUST_TERR_PROD_TEMP""")

---spark.sql("""create table all_all_b_usa_crmods_qa3.ODS_CPIC_HCP_CUST_TERR_PROD_TEMP like all_all_b_usa_crmods_qa3.ODS_CPIC_HCP_CUST_TERR_PROD""")

spark.sql("""insert into all_all_b_usa_crmods_qa3.ODS_CPIC_HCP_CUST_TERR_PROD_TEMP select b.customer_id	,b.territory_id	,b.product_id	,b.eligibility_flag,b.valuation_flag,b.cycle_start_date,b.cycle_end_date,b.batch_id_insert	,b.rec_insert_by	,b.rec_modify_date,b.rec_insert_date,b.batch_id_update	,b.rec_modify_by	,b.cust_sf_id	,b.terr_sf_id	,b.prod_sf_id from (select customer_id	,territory_id	,product_id	,eligibility_flag,valuation_flag,cycle_start_date,cycle_end_date,batch_id_insert	,rec_insert_by	,rec_modify_date,rec_insert_date,batch_id_update	,rec_modify_by	,cust_sf_id	,terr_sf_id	,prod_sf_id,ROW_NUMBER() over (partition by customer_id	,territory_id,product_id order by rec_modify_date) RowNumber from all_all_b_usa_crmods_qa3.ODS_CPIC_HCP_CUST_TERR_PROD a) b where RowNumber=1 """)

spark.sql("select * from all_all_b_usa_crmods_qa3.ODS_CPIC_HCP_CUST_TERR_PROD").count
--9028994

spark.sql("select * from all_all_b_usa_crmods_qa3.ODS_CPIC_HCP_CUST_TERR_PROD_TEMP").count
--8001098

spark.sql("""truncate table all_all_b_usa_crmods_qa3.ods_cpic_hcp_cust_terr_prod""")

spark.sql("""insert into all_all_b_usa_crmods_qa3.ODS_CPIC_HCP_CUST_TERR_PROD select distinct * from all_all_b_usa_crmods_qa3.ODS_CPIC_HCP_CUST_TERR_PROD_TEMP""")

	
===========================================================================================================================================================


spark.sql("""truncate table all_all_b_usa_crmods_qa3.ods_cpic_hcp_cust_terr_addr_temp""")

---spark.sql("""create table all_all_b_usa_crmods_qa3.ods_cpic_hcp_cust_terr_addr_temp like all_all_b_usa_crmods_qa3.ods_cpic_hcp_cust_terr_addr""")

spark.sql("""insert into all_all_b_usa_crmods_qa3.ods_cpic_hcp_cust_terr_addr_temp select b.customer_id	,b.territory_id	,b.addr_id	,b.cycle_start_date,b.cycle_end_date,b.in_territory	,b.batch_id_insert	,b.rec_insert_by	,b.rec_modify_date,b.rec_insert_date,b.batch_id_update	,b.rec_modify_by	,b.cust_sf_id	,b.terr_sf_id	,b.addr_sf_id from (select customer_id	,territory_id	,addr_id	,cycle_start_date,cycle_end_date,in_territory	,batch_id_insert	,rec_insert_by	,rec_modify_date,rec_insert_date,batch_id_update	,rec_modify_by	,cust_sf_id	,terr_sf_id	,addr_sf_id,ROW_NUMBER() over (partition by customer_id	,territory_id,addr_id order by rec_modify_date) RowNumber from all_all_b_usa_crmods_qa3.ods_cpic_hcp_cust_terr_addr a) b where RowNumber=1 """)

spark.sql("select * from all_all_b_usa_crmods_qa3.ods_cpic_hcp_cust_terr_addr").count
--16602102
spark.sql("select * from all_all_b_usa_crmods_qa3.ods_cpic_hcp_cust_terr_addr_temp").count
--16473053
spark.sql("""truncate table all_all_b_usa_crmods_qa3.ods_cpic_hcp_cust_terr_addr""")

spark.sql("""insert into all_all_b_usa_crmods_qa3.ods_cpic_hcp_cust_terr_addr select distinct * from all_all_b_usa_crmods_qa3.ods_cpic_hcp_cust_terr_addr_temp""")

===========================================================================================================================================================

spark.sql("""truncate table all_all_b_usa_crmods_qa3.ods_cpic_product_alignment_temp""")

---spark.sql("""create table all_all_b_usa_crmods_qa3.ods_cpic_product_alignment_temp like all_all_b_usa_crmods_qa3.ods_cpic_product_alignment""")

spark.sql("""insert into all_all_b_usa_crmods_qa3.ods_cpic_product_alignment_temp select b.product_id	,b.territory_id,b.cycle_start_date,b.cycle_end_date,b.batch_id_insert	,b.rec_insert_by	,b.rec_modify_date,b.rec_insert_date,b.batch_id_update	,b.rec_modify_by,b.prod_sf_id,b.terr_sf_id from (select product_id	,territory_id,cycle_start_date,cycle_end_date,batch_id_insert	,rec_insert_by	,rec_modify_date,rec_insert_date,batch_id_update	,rec_modify_by	,prod_sf_id,terr_sf_id,ROW_NUMBER() over (partition by product_id	,territory_id order by rec_modify_date) RowNumber from all_all_b_usa_crmods_qa3.ods_cpic_product_alignment a) b where RowNumber=1 """)

spark.sql("select * from all_all_b_usa_crmods_qa3.ods_cpic_product_alignment").count
--7385
spark.sql("select * from all_all_b_usa_crmods_qa3.ods_cpic_product_alignment_temp").count
--6865
spark.sql("""truncate table all_all_b_usa_crmods_qa3.ods_cpic_product_alignment""")

spark.sql("""insert into all_all_b_usa_crmods_qa3.ods_cpic_product_alignment select distinct * from all_all_b_usa_crmods_qa3.ods_cpic_product_alignment_temp""")


===========================================================================================================================================================

spark.sql("""truncate table all_all_b_usa_crmods_qa3.ods_cpic_hco_cust_terr_prod_temp""")

---spark.sql("""create table all_all_b_usa_crmods_qa3.ods_cpic_hco_cust_terr_prod_temp like all_all_b_usa_crmods_qa3.ods_cpic_hco_cust_terr_prod""")

spark.sql("""insert into all_all_b_usa_crmods_qa3.ods_cpic_hco_cust_terr_prod_temp select b.customer_id	,b.territory_id	,b.product_id	,b.eligibility_flag,b.valuation_flag,b.cycle_start_date,b.cycle_end_date,b.batch_id_insert	,b.rec_insert_by	,b.rec_modify_date,b.rec_insert_date,b.batch_id_update	,b.rec_modify_by	,b.cust_sf_id	,b.terr_sf_id	,b.prod_sf_id from (select customer_id	,territory_id	,product_id	,eligibility_flag,valuation_flag,cycle_start_date,cycle_end_date,batch_id_insert	,rec_insert_by	,rec_modify_date,rec_insert_date,batch_id_update	,rec_modify_by	,cust_sf_id	,terr_sf_id	,prod_sf_id,ROW_NUMBER() over (partition by customer_id	,territory_id,product_id order by rec_modify_date) RowNumber from all_all_b_usa_crmods_qa3.ods_cpic_hco_cust_terr_prod a) b where RowNumber=1 """)

spark.sql("select * from all_all_b_usa_crmods_qa3.ods_cpic_hco_cust_terr_prod").count
--33655910
spark.sql("select * from all_all_b_usa_crmods_qa3.ods_cpic_hco_cust_terr_prod_temp").count
--15394814
spark.sql("""truncate table all_all_b_usa_crmods_qa3.ods_cpic_hco_cust_terr_prod""")

spark.sql("""insert into all_all_b_usa_crmods_qa3.ods_cpic_hco_cust_terr_prod select distinct * from all_all_b_usa_crmods_qa3.ods_cpic_hco_cust_terr_prod_temp""")

===========================================================================================================================================================

spark.sql("""truncate table all_all_b_usa_crmods_qa3.ods_cpic_hco_cust_terr_addr_temp""")

---spark.sql("""create table all_all_b_usa_crmods_qa3.ods_cpic_hco_cust_terr_addr_temp like all_all_b_usa_crmods_qa3.ods_cpic_hco_cust_terr_addr""")

spark.sql("""insert into all_all_b_usa_crmods_qa3.ods_cpic_hco_cust_terr_addr_temp select b.customer_id	,b.territory_id	,b.addr_id	,b.cycle_start_date,b.cycle_end_date,b.in_territory	,b.batch_id_insert	,b.rec_insert_by	,b.rec_modify_date,b.rec_insert_date,b.batch_id_update	,b.rec_modify_by	,b.cust_sf_id	,b.terr_sf_id	,b.addr_sf_id from (select customer_id	,territory_id	,addr_id	,cycle_start_date,cycle_end_date,in_territory	,batch_id_insert	,rec_insert_by	,rec_modify_date,rec_insert_date,batch_id_update	,rec_modify_by	,cust_sf_id	,terr_sf_id	,addr_sf_id,ROW_NUMBER() over (partition by customer_id	,territory_id,addr_id order by rec_modify_date) RowNumber from all_all_b_usa_crmods_qa3.ods_cpic_hco_cust_terr_addr a) b where RowNumber=1 """)

spark.sql("select * from all_all_b_usa_crmods_qa3.ods_cpic_hco_cust_terr_addr").count
--3468930
spark.sql("select * from all_all_b_usa_crmods_qa3.ods_cpic_hco_cust_terr_addr_temp").count
--2248744
spark.sql("""truncate table all_all_b_usa_crmods_qa3.ods_cpic_hco_cust_terr_addr""")

spark.sql("""insert into all_all_b_usa_crmods_qa3.ods_cpic_hco_cust_terr_addr select distinct * from all_all_b_usa_crmods_qa3.ods_cpic_hco_cust_terr_addr_temp""")

