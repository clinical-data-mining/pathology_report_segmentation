-- ddp_pathology_tissue.sql
-- This script will pull pathology tissue data from Darwin using the DCMSPT schema. This data reflects the output of the DDP API.
-- pathology_tissue = """
with
pt as (select pt_mrn mrn, PT_PT_DEIDENTIFICATION_ID pid from DCMSPT.patient_demographics_v d )
--------------------------------------------------------------------------------------------------------------------------------

, pid as (select pid, mrn, 'IMPRESSION:' as imp_anchor, 'DICTATED BY:' as dic_anchor , 'IMPRESSION:[\w\W]*(?=DICTATED ?BY:)' regExptr from pt
)--select * from pid


, PATH_RPRT_TYPE_LOOKUP as (select PRPTLK_PRPT_TYPE_CD, PRPTLK_PRPT_TYPE_DESC from "DV"."PATH_RPRT_TYPE_LOOKUP" union select 'DS_R','Surgical Pathology' from sysibm.sysdummy1)
, path_rpt_type as (select t.*, case when upper(rtrim("PRPTLK_PRPT_TYPE_DESC")) like '%SURGICAL%' then 'PATHSURG' when upper(rtrim("PRPTLK_PRPT_TYPE_DESC")) like '%DIAGNOSTIC MOLECULAR%' then 'PATHDM' when upper(rtrim("PRPTLK_PRPT_TYPE_DESC")) like '%CYTOGENETICS%' then 'PATHCYTOG' when upper(rtrim("PRPTLK_PRPT_TYPE_DESC")) like '%CYTOLOGY%' then 'PATHCYTO' else 'PATHOTHER' end as PATH_TYPE_ROLLUP from PATH_RPRT_TYPE_LOOKUP t)--select * from path_rpt_type
, path_refs_pre as (select PRPTR_PT_DEIDENTIFICATION_ID, PRPTR_PATH_RPT_ID, PRPTR_CHILD_PATH_RPT_ID from DCMSPT.PATHOLOGY_REPORT_REFERENCES union all select PRPTR_PT_DEIDENTIFICATION_ID, PRPTR_CHILD_PATH_RPT_ID PRPTR_PATH_RPT_ID , PRPTR_PATH_RPT_ID PRPTR_CHILD_PATH_RPT_ID from DCMSPT.PATHOLOGY_REPORT_REFERENCES )
, path_dt_pre as (
         select pid, mrn, trim(prpt_accession_no) prpt_accession_no, prpt_report report_text, PRPT_PATH_RPT_ID, PATH_TYPE_ROLLUP
         , "t3".dt_dte as Procedure_Date
         , case when "t17"."PRPT_PROCEDURE_DATE_ID" = 9000001 then "t17"."PRPT_REPORT_DATE_ID" else "t17"."PRPT_PROCEDURE_DATE_ID" end proc_date_id
         FROM pid "t1"
         INNER JOIN "DV"."PATHOLOGY_REPORT_V" "t17" ON "t1".pid = "t17"."PRPT_PT_DEIDENTIFICATION_ID"
         inner JOIN path_rpt_type "t2" ON "t17".PRPT_REPORT_TYPE = "t2"."PRPTLK_PRPT_TYPE_CD"
         inner JOIN "DV"."DATE_V" "t3" ON case when "t17"."PRPT_PROCEDURE_DATE_ID" = 9000001 then "t17"."PRPT_REPORT_DATE_ID" else "t17"."PRPT_PROCEDURE_DATE_ID" end = "t3"."DT_DATE_ID"
)-- select * from path_dt_pre
, path_dts as (
     select p.*  from path_dt_pre p
)-- select * from path_dts

, dates_to_disp as (
 --Dates to show for surgical path reports
     select pid, mrn, Procedure_Date, proc_date_id from path_dts path where path_type_rollup = 'PATHSURG'
     union
     --Dates to show for reports with no associated surgical path
     select pid, mrn, Procedure_Date, proc_date_id from path_dts path left join
     (select PRPTR_PATH_RPT_ID, PRPTR_CHILD_PATH_RPT_ID , path_type_rollup from DCMSPT.PATHOLOGY_REPORT_REFERENCES join path_dts on PRPTR_PATH_RPT_ID = PRPT_PATH_RPT_ID where path_type_rollup = 'PATHSURG') r on PRPTR_CHILD_PATH_RPT_ID = PRPT_PATH_RPT_ID where PRPTR_CHILD_PATH_RPT_ID is null
)-- select * from dates_to_disp

, $PATH$dmp_dx as (
     select x.* from (
     select distinct t.* , dt.PDRX_PATH_RPT_ID
     from
     path_dts inner join DCMSPT.PATHOLOGY_DMP_RESULTS_XML dt on dt.PDRX_PATH_RPT_ID = PRPT_PATH_RPT_ID
     cross join xmltable('$c/root/*' passing dt.PDRX_XML_VALUE as "c" columns
     dmp_patient_id varchar(264) path '../meta-data/dmp_patient_id' ,
     dmp_sample_id varchar(264) path '../meta-data/dmp_sample_id' ,
     primary_site varchar(264) path '../meta-data/primary_site' ,
     tumor_type_name varchar(264) path '../meta-data/tumor_type_name' ,
     metastasis_site varchar(264) path '../meta-data/metastasis_site'
     ) t
     ) x
)--select * from $PATH$dmp_dx

, mdx_annots as (
     select distinct dmp.PRPT_PATH_RPT_ID "MDPRA_PRPT_PATH_RPT_ID", TEST_ID , "GENE", RESULT, LOCUS, AA_CHANGE SEARCH_HIT2A, NUCLEOTIDE SEARCH_HIT2B, SEARCH_HIT1 , SEARCH_HIT2, METHOD
     , aberrationtype aberration_type
     , PRPT_MODIFIED_DT "MDPRA_MODIFIED_DT"
     from pid
     inner join DCMSPT.PATHOLOGY_STRUCTURED_RESULTS_V dmp on pid = PT_PT_DEIDENTIFICATION_ID
)--select * from mdx_annots

, path_pre as (
     select pid, mrn, trim(prpt_accession_no) prpt_accession_no
     , case
     when PATH_TYPE_ROLLUP = 'PATHSURG' then dv.regExStr('(?i)Specimens ?Submitted:' || chr(13) || chr(10) || '[\w\W]*?(?=\r\n(AMENDED )?DIAGNOSIS:|\r\n[ ]*?(AMENDED )?DIAGNOSIS:)', prpt_report)
     when PATH_TYPE_ROLLUP in ( 'PATHDM' ) then dv.regExStr('(?i)Specimens ?Submitted:' || chr(13) || chr(10) || '[\w\W]*?(?=\r\n\r\n)', prpt_report)
     when PATH_TYPE_ROLLUP = 'PATHCYTO' then dv.regExStr('(?i)Specimen Description:' || chr(13) || chr(10) || '[\w\W]*?(?=\r\n(CYTOLOGIC)? DIAGNOSIS:)', prpt_report)
     when PATH_TYPE_ROLLUP in ( 'PATHCYTOG' ) and dv.regExStr('(?i)Specimens ?Submitted:' || chr(13) || chr(10) || '[\w\W]*?(?=\r\n\r\n)', prpt_report) <> ''
     then dv.regExStr('(?i)Specimens ?Submitted:' || chr(13) || chr(10) || '[\w\W]*?(?=\r\n\r\n)', prpt_report)
     when PATH_TYPE_ROLLUP in ( 'PATHCYTOG' ) and dv.regExStr('(?i)Specimens ?Submitted:' || chr(13) || chr(10) || '[\w\W]*?(?=\r\n\r\n)', prpt_report) = ''
     then dv.regExStr('(?i)Specimens ?Submitted: *' || chr(13) || chr(10) || '[\w\W]*?(?=\r\n)', prpt_report)
     else '' end Spec_sub_pre
     , prpt_report report_text, PRPT_PATH_RPT_ID, PATH_TYPE_ROLLUP
     , "t3".dt_dte as Procedure_Date, "t3"."DT_DATE_ID" proc_date_id
     , d.*
     FROM pid "t1"
     INNER JOIN "DV"."PATHOLOGY_REPORT_V" "t17" ON "t1".pid = "t17"."PRPT_PT_DEIDENTIFICATION_ID"
     inner JOIN path_rpt_type "t2" ON "t17".PRPT_REPORT_TYPE = "t2"."PRPTLK_PRPT_TYPE_CD"
     inner JOIN "DV"."DATE_V" "t3" ON case when "t17"."PRPT_PROCEDURE_DATE_ID" = 9000001 then "t17"."PRPT_REPORT_DATE_ID" else "t17"."PRPT_PROCEDURE_DATE_ID" end = "t3"."DT_DATE_ID"
     --left join panels on prpt_path_rpt_id = rpt_id
     left join $PATH$dmp_dx d on prpt_path_rpt_id = d.PDRX_PATH_RPT_ID
)--select * from path_pre

, path as (
 select p.*--, r.total_ct, r.pos_ct, r.neg_ct, r.other_ct
 , trim(replace(replace(
 dv.regExStr('(?i)(?<=Specimens? ?(Description|Submitted):?\r\n)[\w\W]*',Spec_sub_pre)
 , chr(13),'\r'),chr(10),'\n'))
 as Spec_sub

 , dv.regExStr('(?i)(?<=DOP:)\d{1,2}/\d{1,2}/\d{1,4}',Spec_sub_pre) as parsed_dmp_DOP
 from path_pre p --left join count_of_existing_results r on p.PRPT_PATH_RPT_ID = r.PRPT_PATH_RPT_ID
) -- select * from path where prpt_accession_no = 'M06-2207'

, dmp_child as (
 select PRPTR_PATH_RPT_ID, PRPTR_CHILD_PATH_RPT_ID from DCMSPT.PATHOLOGY_REPORT_REFERENCES join path dmp on PRPTR_CHILD_PATH_RPT_ID = dmp.PRPT_PATH_RPT_ID --and dmp.path_type_rollup = 'PATHDM'
)-- select * from dmp_child

, dmp_parentless as (
-- get all reports that don't have associated Path, need to include these too
 select PRPT_PATH_RPT_ID from path left join
 (
 select PRPTR_PATH_RPT_ID, PRPTR_CHILD_PATH_RPT_ID , path_type_rollup from DCMSPT.PATHOLOGY_REPORT_REFERENCES join path_dts on PRPTR_PATH_RPT_ID = PRPT_PATH_RPT_ID where path_type_rollup = 'PATHSURG'
 ) r on PRPTR_CHILD_PATH_RPT_ID = PRPT_PATH_RPT_ID where PRPTR_CHILD_PATH_RPT_ID is null
)-- select * from dmp_parentless

, path2a as (
     select surg.pid, surg.mrn, surg.path_type_rollup, surg.PRPT_PATH_RPT_ID as rpt_id, surg.Procedure_Date, surg.proc_date_id
     , surg.prpt_accession_no accession_no
     , surg.report_text report_text, replace(surg.Spec_sub, '"','\"') as spec
     -- , dmp_rpt.prpt_accession_no dmp_accession_no, dmp_rpt.report_text as dmp_report_text,
     , '{"type" : "' || surg.path_type_rollup || '", "deid" : "' || surg.prpt_path_rpt_id || '", "id" : "' || surg.prpt_accession_no || '", "label" : "' || surg.prpt_accession_no || '" '
     || ', "specimen" : "' || replace(replace(surg.Spec_sub, '\\', '\\\\'), '"', '\\\"') || '"'
     || '}' as details
     from path surg --left join dmp_child dmp on surg.PRPT_PATH_RPT_ID = PRPTR_PATH_RPT_ID
     --left join path dmp_rpt on PRPTR_CHILD_PATH_RPT_ID = dmp_rpt.prpt_path_rpt_id and dmp_rpt.path_type_rollup = 'PATHDM'
     where surg.path_type_rollup = 'PATHSURG'
     /* PATHDM with parent surgpath i.e. use surg path procedure date, not dm path report's */
     union all
     select surg.pid, surg.mrn, dmp_rpt.path_type_rollup, dmp.PRPTR_CHILD_PATH_RPT_ID as rpt_id, surg.Procedure_Date, surg.proc_date_id
     , dmp_rpt.prpt_accession_no accession_no
     , dmp_rpt.report_text report_text, null as spec
     --, dmp_rpt.prpt_accession_no dmp_accession_no, dmp_rpt.report_text as dmp_report_text
     , '{"type" : "' || dmp_rpt.path_type_rollup || '", "deid" : "' || dmp_rpt.prpt_path_rpt_id || '", "id" : "' || dmp_rpt.prpt_accession_no || '", "label" : "' || dmp_rpt.prpt_accession_no || '" '
     || ', "specimen" : "' || replace(replace(dmp_rpt.Spec_sub, '\\', '\\\\'), '"','\\\"') || '"'
     || '}'
     as details
     from path surg left join dmp_child dmp on surg.PRPT_PATH_RPT_ID = PRPTR_PATH_RPT_ID
     join path dmp_rpt on PRPTR_CHILD_PATH_RPT_ID = dmp_rpt.prpt_path_rpt_id and dmp_rpt.path_type_rollup = 'PATHDM'
     where surg.path_type_rollup = 'PATHSURG'
     /* CYTO with parent surgpath i.e. use surg path procedure date, not CYTO report's */
     union all
     select surg.pid, surg.mrn, dmp_rpt.path_type_rollup, dmp.PRPTR_CHILD_PATH_RPT_ID as rpt_id, surg.Procedure_Date, surg.proc_date_id
     , dmp_rpt.prpt_accession_no accession_no
     , dmp_rpt.report_text report_text, null as spec
     --, dmp_rpt.prpt_accession_no dmp_accession_no, dmp_rpt.report_text as dmp_report_text
     , '{"type" : "' || dmp_rpt.path_type_rollup || '", "deid" : "' || dmp_rpt.prpt_path_rpt_id || '", "id" : "' || dmp_rpt.prpt_accession_no || '", "label" : "' || dmp_rpt.prpt_accession_no || '" '
     || ', "specimen" : "' || replace(replace(dmp_rpt.Spec_sub, '\\', '\\\\'), '"','\\\"') || '"'
     || '}'
     as details
     from path surg left join dmp_child dmp on surg.PRPT_PATH_RPT_ID = PRPTR_PATH_RPT_ID
     join path dmp_rpt on PRPTR_CHILD_PATH_RPT_ID = dmp_rpt.prpt_path_rpt_id and dmp_rpt.path_type_rollup not in ( 'PATHDM' ,'PATHSURG')
     where surg.path_type_rollup = 'PATHSURG'
     union all
     /* rpts without parent surgpath i.e. use this path report's procedure date b/c no parent*/
     select dmp_rpt.pid, dmp_rpt.mrn,dmp_rpt.path_type_rollup,dmp_rpt.prpt_path_rpt_id rpt_id, dmp_rpt.Procedure_Date, dmp_rpt.proc_date_id
     , dmp_rpt.prpt_accession_no accession_no
     , dmp_rpt.report_text report_text, replace(dmp_rpt.Spec_sub, '"','\"') as spec
     , '{"type" : "' || dmp_rpt.path_type_rollup || '", "deid" : "' || dmp_rpt.prpt_path_rpt_id || '", "id" : "' || dmp_rpt.prpt_accession_no || '", "label" : "' || dmp_rpt.prpt_accession_no || '" '
     || ', "specimen" : "' || replace(replace(dmp_rpt.Spec_sub, '\\', '\\\\'), '"','\\\"') || '"'
     || '}' details
     from path dmp_rpt join dmp_parentless p on p.prpt_path_rpt_id = dmp_rpt.prpt_path_rpt_id
     where dmp_rpt.path_type_rollup <> 'PATHSURG' -- these are included in the first query of this union
) --select * from path2a

-- list of procedures supplied by Dr. juluru, used to limit to biopsies only

---- replacing this with hard coded values
--, cdr as ( select * from dvidic.RAD10341_rad_path_proc join dv.RADIOLOGY_ANCILLARY_CODES on RAC_RAD_ANC_CODE = code)
,cdr ( code ) as (values
('ABSCON'),('ABSDRABDCT'),('ABSDRABDUS'),('ABSDRABFL'),('ABSDRCHCT'),('ABSDRCHUS'),('ABSDRLIVCT'),('ABSDRLIVER'),('ABSDRLIVUS'),('ABSDRNSTCT'),('ABSDRNSTFL'),('ABSDRSTUS'),('ASPIRABDCT'),('ASPIRABDFL'),('ASPIRABDUS'),('ASPIRCHCT'),('ASPIRCHFL'),('ASPIRCHUS'),('ASPIRLIV'),('ASPIRLIVCT'),('ASPIRLIVUS'),('ASPIRSTCT'),('ASPIRSTFL'),('ASPIRSTUS'),('BRASPR'),('BRAXBX2L'),('BRAXBX2R'),('BRAXBXB'),('BRAXBXL'),('BRAXBXR'),('BRBXAXFNAB'),('BRBXAXFNAL'),('BRBXAXFNAR'),('BRBXB'),('BRBXL'),('BRBXR'),('CFSUSBXST'),('USBXFL1'),('USBXFL2'),('USBXFL3'),('USBXFL4'),('USBXFS1'),('USBXFS2'),('USBXFS3'),('USBXFS4'),('USBXST1'),('USBXST2'),('USBXST3'),('BRSTER2L'),('BRSTER2R'),('BRSTER3L'),('BRSTER3R'),('BRSTERB'),('BRSTERB3'),('BRSTERB4'),('BRSTERB5'),('BRSTERL'),('BRSTERR'),('BRUSASP2L'),('BRUSASP2R'),('BRUSASP3L'),('BRUSASP3R'),('BRUSASPB'),('BRUSASPB3'),('BRUSASPB4'),('BRUSASPB5'),('BRUSBX2AL'),('BRUSBX2AR'),('BRUSBX2L'),('BRUSBX2R'),('BRUSBX3A2R'),('BRUSBX3A3L'),('BRUSBXA2R'),('ASPJOINT'),('ASUSBXREN'),('BDRAIN'),('BDUCTBX'),('BRASPB'),('BRASPL'),('BRUSASPL'),('BRUSASPR'),('BRUSBX2A2L'),('BRUSBX2A2R'),('BRUSBX2A3L'),('BRUSBX2A3R'),('BRUSBX3A3R'),('BRUSBX3AL'),('BRUSBX3AR'),('BRUSBX3L'),('BRUSBX3R'),('BRUSBXA2L'),('BRUSBXB'),('BRUSBXB3'),('BRUSBXB4'),('BRUSBXB5'),('BRUSBXL'),('BRUSBXR'),('USBXST4'),('BXBNE'),('BXCHEST'),('BXKIDNEY'),('BXLIV'),('BXPELV'),('CSUSBXST'),('EPIDASPIR'),('EPIDURDRG'),('GLUMINBX'),('KUSBCF'),('KUSBCFMD'),('KUSBSP'),('KUSBSPMD'),('LPDN'),('MEDIABXPET'),('MRBRBX2L'),('MRBRBX2R'),('MRBRBXB'),('NBABDRETUS'),('NBADRENCT'),('NBADRENMR'),('NBADRENPET'),('NBBONE'),('NBBONEMR'),('NBMEDSTCT'),('NBNECKMR'),('NBNECKPET'),('NBNECKUS'),('NBNEUSTCT'),('NBNEUSTMR'),('NBNEUSTPET'),('NBNEUSTUS'),('NBPELMR'),('PARASPDRG'),('PARSPASMR'),('PCARDCHG'),('PCARDCK'),('PCARDREPL'),('PCARDRG'),('USTHYBIOP'),('BRFLOWR'),('BRFLOWL'),('BRFLOWB'),('BRAXFNA2L'),('BRAXFNA2R'),('BRAXFNAB'),('BRAXFNAB3'),('BRAXFNAB4'),('BRAXFNAL'),('NBABDREPET'),('NBABDRETCT'),('NBABDRETMR'),('NBBONEPET'),('NBBREASTMR'),('NBFACECT'),('NBFACEMR'),('NBFACEPET'),('NBKIDNEYMR'),('NBPELPET'),('NBPELUS'),('NBSIPINEMR'),('NBSPINEMR'),('NBSPINEPET'),('NBSTCT'),('PCARDRGUS'),('PLEURX'),('PLEURXCH'),('PLEURXCK'),('PLEURXREM'),('SPB'),('BRUSBXA3L'),('BRUSBXA3R'),('BRUSBXAB'),('BRUSBXAL'),('BRUSBXAR'),('BRAXFNAR'),('MRBRBXB3'),('MRBRBXB4'),('MRBRBXL'),('MRBRBXR'),('MRBX'),('NBKIDPET'),('NBLIVERMR'),('NBLIVERUS'),('NBLIVPET'),('NBLUNG'),('NBLUNGPET'),('NBSTMR'),('NBSTPET'),('NBSTUS'),('NKB'),('OBIOPS'),('PARASPASP'),('THORDRAIN'),('TJLB'),('USABSDRN'),('USBXBODY'),('USBXTHY'),('USBXTHYB')
)-- select * from cdr

, rad as (
 SELECT t1.pid, t1.MRN, dv.regExStr('(?i)'|| regExptr,RRPT_REPORT_TXT) impression, RRPT_RPT_ID,
 trim(t2.RRPT_ACCESSION_NO) AS "Radiology Accession Number", t2.RRPT_ACCT_NO AS "Radiology Report Accout Number", t2.RRPT_PERFORMED_DTE AS "Radiology Performed Date", t2.RRPT_REPORT_DTE AS "Radiology Report Date",
 t2.RRPT_PROCEDURE_NAME AS "Radiology Procedure Name", replace(t2.RRPT_PROCEDURE_NAME, dv.regExStr('(?i) w/o?(&w/o)? ?(con|video)',t2.RRPT_PROCEDURE_NAME), '') AS "Radiology Procedure Name Brief",
 RRPT_ANC_CODE, t2.RRPT_ANC_CODE AS "Radiology Ancillary Code",
 t2.RRPT_REQUESTER AS "Radiology Order Requester",
 lower(trim(ord_med.mp_last_nm)) "Rad Order Requester Last Name",
 lower(trim(ord_med.mp_first_nm)) "Rad Order Requester First Name",
 t2.RRPT_ORDER_STATUS_DESC AS "Radiology Order Status"
 , MP_EMAIL_ADDR "Requester Email"
 , case when length(RRPT_REPORT_TXT) <= 32000 then rtrim(cast(substr(RRPT_REPORT_TXT, 1, 32000) as varchar(32000))) else
 cast(substr(RRPT_REPORT_TXT, 1, 32000) as varchar(32000))
 end "path_prpt_p1" ,
 case when length(RRPT_REPORT_TXT) > 32000 then rtrim(cast(substr(RRPT_REPORT_TXT, 32001, 32000) as varchar(32000))) else ''
 end "path_prpt_p2"
 FROM
 pid t1
 inner JOIN DCMSPT.RADIOLOGY_REPORT_V t2 ON t1.pid = t2.RRPT_PT_DEIDENTIFICATION_ID
 -- list of procedures supplied by Dr. juluru, used to limit to biopsies only
 inner join cdr on t2.RRPT_ANC_CODE = cdr.code
 join DCMSPT.medpro ord_med on t2.RRPT_REQUESTER_MEDPRO_ID = MP_MEDPRO_ID
 order by RRPT_PERFORMED_DTE desc
)
--select * from rad

, rad2 as (
 select pid, MRN, RRPT_RPT_ID, impression, "Radiology Accession Number", "Radiology Report Accout Number", "Radiology Performed Date", "Radiology Report Date",
 "Radiology Procedure Name", "Radiology Procedure Name Brief", "Radiology Ancillary Code"
 , upper(left("Rad Order Requester Last Name",1)) || substr("Rad Order Requester Last Name",2) "Rad Order Requester Last Name"
 , upper(left("Rad Order Requester First Name",1)) || substr("Rad Order Requester First Name",2) "Rad Order Requester First Name"
 , "Radiology Order Requester", "Requester Email", "Radiology Order Status"
 , replace("path_prpt_p1", impression, '<span class="rad_impress txthighlight">' || impression || '</span>') "path_prpt_p1"
 , "path_prpt_p2"
 from rad
)
, rad3 as (
 select pid, mrn, "Radiology Performed Date", trim("Radiology Procedure Name Brief") proc_desc
 , '{"type" : "Radiology", "deid" : "'|| cast(RRPT_RPT_ID as varchar(32)) || '", "id" : "' || "Radiology Accession Number" || '", "label" : "' || replace(trim("Radiology Procedure Name Brief"),'"','\"') || '" }' as details
 from rad2
)-- select * from rad3

--, optime as (
-- SELECT pid, SSE_MRN AS MRN,
-- SSE_SURG_DTE AS Procedure_Date,
-- SSE_PT_TYPE AS "IP/OP Indicator",
-- SSP_PROC_CPT4_CD AS "OpTime CPT Code",
-- SSP_PROC_NAME AS "Procedure Desc",
-- rtrim(SSP_SURG_LAST_NM) || ',' || SSP_SURG_FIRST_NM AS "Physician"
-- , SSP_PROC_SEQ_NO
-- FROM pid inner join
-- IDB.SRG_SURG_EVENT on sse_mrn = pid.mrn
-- JOIN IDB.SRG_SURG_PROCEDURE ON SSE_LOG_ID = SSP_LOG_ID
--) --
--, optime2 as (
-- select pid, mrn, procedure_date
-- , trim("Procedure Desc") as "Procedure Desc"
-- , '{"type" : "Surgery", "id" : "" , "label" : "' || replace(trim("Procedure Desc"),'"','\"') || '" }' as details
-- from optime
--)--select * from optime2
--, optime3 as (
-- select o.*, case when max(locate(
-- case when left(upper(r.proc_desc),3) = 'IR ' then substr(r.proc_desc,4) else r.proc_desc end, "Procedure Desc")) > 0 then 1 else 0 end omit_bc_rad_match
-- from optime2 o left join rad3 r on o.pid = r.pid and r."Radiology Performed Date" = o.procedure_date
-- group by o.pid, o.mrn, o.procedure_date, o.details, o."Procedure Desc"
--)-- select * from optime3
, optime_and_rad_dates as (
 select pid, mrn,"Radiology Performed Date" procedure_date, details from rad3
-- union all
-- select pid, mrn, procedure_date, details from optime3 where omit_bc_rad_match = 0
)-- select * from optime_and_rad_dates

--, pbd as (
-- SELECT Distinct
-- --rownumber() over (partition by t1.PT_PT_DEIDENTIFICATION_ID order by t4."DT_DTE" desc) rn,
-- PID,
-- rtrim(PROCEDURE_CODE_CATALOG1."PCC_CODE") AS "CPT Code",
-- rtrim(PROCEDURE_CODE_CATALOG1."PCC_DESC") AS Procedure_Description,
-- trim(PROCEDURE_CODE_CATALOG.PCC_CODE) as "PBD Code",
-- trim(PROCEDURE_CODE_CATALOG.PCC_DESC) as "Extended Description",
-- rtrim(t3."MP_FULL_NM") AS "Physician",
-- rtrim(t3."MP_SERVICE_DESC") AS "Phy Service",
-- rtrim(MP_ACADEMIC_DEPT_DESC) as "Phy Dept",
-- t4."DT_DTE" AS Procedure_Date,
-- t4."DT_YEAR" AS "Proc Year",
-- PATIENT_PROCEDURE."PTX_INPATIENT_OUTPATIENT_IND" AS "Proc IP-OP"
-- , MP_EMAIL_ADDR "Physician Email" , MP_FIRST_NM
-- , case when substr(trim(MP_LAST_NM),2,1) = '''' then upper(left(trim(MP_LAST_NM),3)) || lower(substr(trim(mp_last_nm),4))
-- else upper(left(trim(MP_LAST_NM),1)) || lower(substr(trim(mp_last_nm),2))
-- end mp_last_nm
-- FROM pid t1
-- inner JOIN DCMSPT.PATIENT_DEIDENTIFIED t6 ON t1.pid = t6.PDI_PT_DEIDENTIFICATION_ID
-- inner join DCMSPT.PATIENT_PROCEDURE AS PATIENT_PROCEDURE ON t6.PDI_PT_DEIDENTIFICATION_ID = PATIENT_PROCEDURE.PTX_PT_DEIDENTIFICATION_ID
-- inner join DCMSPT.PROCEDURE_CODE_CATALOG AS PROCEDURE_CODE_CATALOG on PROCEDURE_CODE_CATALOG.PCC_ID = PATIENT_PROCEDURE.PTX_PCC_ID AND PROCEDURE_CODE_CATALOG.PCC_PROC_TYPE_DESC = 'PBD' AND PATIENT_PROCEDURE.PTX_PROC_SOURCE_DESC = 'PBD'
-- inner join DCMSPT.PROCEDURE_CODE_CATALOG AS PROCEDURE_CODE_CATALOG1 on PROCEDURE_CODE_CATALOG1.PCC_ID = PROCEDURE_CODE_CATALOG.PCC_ASSOCIATED_CPT_PROC_ID
-- inner JOIN DCMSPT.medpro_v t3 ON PATIENT_PROCEDURE.PTX_MEDPRO_ID = t3.MP_MEDPRO_ID
-- inner JOIN DCMSPT.DATE_V t4 ON PATIENT_PROCEDURE.PTX_PROC_DATE_ID = t4.DT_DATE_ID
-- where CASE
-- when PROCEDURE_CODE_CATALOG1."PCC_CODE" between '00100' and '01999' then 0
-- when PROCEDURE_CODE_CATALOG1."PCC_CODE" between '00021' and '69990' then 1 else 0 end = 1
--)-- select * from pbd
--, pbd2 as (
-- select pid, procedure_date -- , Procedure_description
-- , '{"type" : "Phys Bill", "id" : "'|| "PBD Code" || '" , "label" : "' || replace(trim(Procedure_Description),'"','\"') || '" }' as details
-- from pbd
--)-- select * from pbd2

--Select * from IDB.OMS_ORDER where OO_ANCIL_REF_CD = 'M13-8600' and oo_mrn = '00184025'
, dates_to_disp2 as (
     select * from dates_to_disp
     --union  select pid, mrn, oo_enter_dte, dt_date_id  from oms_pending_dmp join DCMSPT.date_v on oo_enter_dte = dt_dte
)-- select * from dates_to_disp2

, path3a as (
     select pid, listagg(distinct spec || '\r\n','') spec, procedure_date, path_type_rollup
     from path2a group by procedure_date, path_type_rollup,pid
)--select * from path3a

, finp as (
     select 'event' as type, d.pid, d.mrn, d.procedure_date,
     '{"date" : "' || varchar_format(d.procedure_date, 'YYYY-MM-DD')
     || '","specimen" : "' || replace(replace(coalesce(sp.spec,dmp.spec,cyto.spec,cytog.spec,oth.spec), '\\', '\\\\'), '"', '\\\"')
     || '","events" : [' || listagg(e.details, ',') || '] }' as details
     from dates_to_disp2 d
     left join (
         select pid, procedure_date, details from path2a
--         union all
--         select pid, procedure_date, details from optime_and_rad_dates
--         union all
--         select pid, procedure_date, details from pbd2
     ) e on e.pid = d.pid and d.procedure_date = e.procedure_date
     left join path3a sp on d.pid = sp.pid and d.procedure_date = sp.procedure_date and sp.path_type_rollup = 'PATHSURG'
     left join path3a dmp on d.pid = dmp.pid and d.procedure_date = dmp.procedure_date and dmp.path_type_rollup = 'PATHDM' and sp.pid is null
     left join path3a cyto on d.pid = cyto.pid and d.procedure_date = cyto.procedure_date and cyto.path_type_rollup = 'PATHCYTO' and sp.pid is null and dmp.pid is null
     left join path3a cytog on d.pid = cytog.pid and d.procedure_date = cytog.procedure_date and cytog.path_type_rollup = 'PATHCYTOG' and sp.pid is null and dmp.pid is null and cyto.pid is null
     left join path3a oth on d.pid = oth.pid and d.procedure_date = oth.procedure_date and oth.path_type_rollup = 'PATHOTHER'and sp.pid is null and dmp.pid is null and cyto.pid is null and cytog.pid is null

     group by d.pid, d.mrn, d.procedure_date, coalesce(sp.spec,dmp.spec,cyto.spec,cytog.spec,oth.spec)
    -- union all
    -- select 'oms' as type, pid, mrn, current date, details from oms_pending
)--select * from finp

-- when displaying the tissue view, tissue sample based on this order of priority:
-- Surg Path
-- DMP
-- Cyto
-- Cytogenetics
-- PathOther

select pid, mrn, type, varchar_format(procedure_date ,'YYYY Mon DD') procedure_date_disp , procedure_date
, details
from finp
with ur
;
-- """