-- ddp_pathology_report.sql
-- This script will pull pathology reports from Darwin using the DCMSPT schema. This data reflects the output of the DDP API
-- pathology_report = """
WITH pid AS (
    SELECT
      PT_PT_DEIDENTIFICATION_ID pid,
      pt_mrn mrn
    FROM     dcmspt.PATIENT_DEMOGRAPHICS_V
	WHERE PT_PT_DEIDENTIFICATION_ID in ()

)

,dmp_sample_id AS (
 SELECT *
 FROM DCMSPT.PATHOLOGY_DMP_RESULTS_XML
 INNER JOIN pid ON pid.pid = PDRX_PT_DEIDENTIFICATION_ID
)

,mdx_annots AS (
	 SELECT a.prpt_path_rpt_id AS MDPRA_PRPT_PATH_RPT_ID, TEST_ID,
	 GENE,
	 RESULT,
	 Locus,
	 A.AA_CHANGE AS SEARCH_HIT2A,
	 A.NUCLEOTIDE AS SEARCH_HIT2B,
	 SEARCH_HIT1,
	 SEARCH_HIT2,
	 Method,
	 AberrationType AS Aberration_Type, A.FOLD_CHANGE,
	 PDMR_CREATED_DT
	 FROM pid
	 INNER JOIN DCMSPT.PATHOLOGY_REPORT_V t1 ON pid = PRPT_PT_DEIDENTIFICATION_ID
	 INNER JOIN DCMSPT.PATHOLOGY_STRUCTURED_RESULTS_V A ON a.prpt_path_rpt_id = t1.prpt_path_rpt_id
) --select * from mdx_annots

, path_refs_pre AS (
 SELECT PRPTR_PT_DEIDENTIFICATION_ID,
 PRPTR_PATH_RPT_ID,
 PRPTR_CHILD_PATH_RPT_ID
 FROM DCMSPT.PATHOLOGY_REPORT_REFERENCES
 UNION ALL
 SELECT PRPTR_PT_DEIDENTIFICATION_ID,
 PRPTR_CHILD_PATH_RPT_ID PRPTR_PATH_RPT_ID,
 PRPTR_PATH_RPT_ID PRPTR_CHILD_PATH_RPT_ID
 FROM DCMSPT.PATHOLOGY_REPORT_REFERENCES
)

, path_refs AS (
 SELECT r.PRPTR_PT_DEIDENTIFICATION_ID,
 PRPTR_PATH_RPT_ID,
 listagg(cast(rpt.prpt_path_rpt_id AS VARCHAR(200)), ',') within GROUP (ORDER BY rpt.prpt_Accession_no) AS Associated_Reports
 FROM path_refs_pre r
 INNER JOIN pid ON pid.pid = PRPTR_PT_DEIDENTIFICATION_ID
 INNER JOIN DCMSPT.pathology_report_v rpt ON r.PRPTR_CHILD_PATH_RPT_ID = rpt.prpt_path_rpt_id
 GROUP BY r.PRPTR_PT_DEIDENTIFICATION_ID,
 PRPTR_PATH_RPT_ID
)
--select * from path_refs
, panels_pre AS (
 SELECT DISTINCT MDPRA_PRPT_PATH_RPT_ID rpt_id,
 method,
 GENE
 FROM mdx_annots
 WHERE GENE <> 'T*B*D'
)

,panels AS (
 SELECT rpt_id,
 method
 FROM panelS_pre
 WHERE gene NOT IN ('?','NO RESULT','')
 GROUP BY RPT_ID,
 method
) -- select * from panels

, path_union AS (
	 SELECT t17.PRPT_PATH_RPT_ID, t17.PRPT_PT_DEIDENTIFICATION_ID, t17.PRPT_REPORT_DATE_ID, t17.PRPT_ACCESSION_NO, t17.PRPT_ORDERING_MEDPRO_ID
	, t17.PRPT_REPORT, t17.PRPT_CREATED_BY, t17.PRPT_DV_PROCESS_DTE, t17.PRPT_CREATED_DT, t17.PRPT_MODIFIED_DT, t17.PRPT_DEID_REPORT, t17.PRPT_DEID_DT, t17.PRPT_REPORT_TYPE, t17.PRPT_PROCEDURE_DATE_ID
	 FROM pid t1
	 INNER JOIN DCMSPT.PATHOLOGY_REPORT_V t17 ON t1.pid = t17.PRPT_PT_DEIDENTIFICATION_ID
) --select * from path_union

, dt AS (
	 SELECT t1.pid p_id,
	 t1.mrn MRN,
	 rtrim(PRPT_ACCESSION_NO) PRPT_ACCESSION_NO,
	 PRPT_REPORT_DATE_ID,
	 PRPT_PROCEDURE_DATE_ID,
	 COALESCE(PRPT_PROCEDURE_DATE_ID,PRPT_REPORT_DATE_ID) AS GEN_PROC_RPT_DATE_ID,
	 COALESCE(t3.DT_DTE,t4.DT_DTE) AS GEN_PATH_DATE,
	 COALESCE(t3.DT_YEAR,t4.DT_YEAR) AS GEN_PATH_YEAR,
	 PRPT_PATH_RPT_ID,
	 PRPT_REPORT_TYPE,
	 PRPTLK_PRPT_TYPE_DESC,
	 CASE
	 WHEN UPPER(rtrim (PRPTLK_PRPT_TYPE_DESC)) LIKE '%SURGICAL%' THEN 'PATHSURG'
	 WHEN UPPER(rtrim (PRPTLK_PRPT_TYPE_DESC)) LIKE '%DIAGNOSTIC MOLECULAR%' THEN 'PATHDM'
	 WHEN UPPER(rtrim (PRPTLK_PRPT_TYPE_DESC)) LIKE '%CYTOGENETICS%' THEN 'PATHCYTOG'
	 WHEN UPPER(rtrim (PRPTLK_PRPT_TYPE_DESC)) LIKE '%CYTOLOGY%' THEN 'PATHCYTO'
	 ELSE 'PATHOTHER'
	 END AS PATH_TYPE_ROLLUP,
	 impact.gene,
	 SEARCH_HIT2A,
	 SEARCH_HIT2B,
	 REPLACE(aberration_type,'_',' ') Aberration_Type,
	 Fold_Change,
	 panels.method,
	 --panels.gene_sum,
	 search_hit1,
	 search_hit2,
	 test_id,
	 RESULT
	 FROM pid t1
	 INNER JOIN path_union t17 ON t1.pid = t17.PRPT_PT_DEIDENTIFICATION_ID
	 LEFT JOIN DCMSPT.PATH_RPRT_TYPE_LOOKUP t2 ON t17.PRPT_REPORT_TYPE = t2.PRPTLK_PRPT_TYPE_CD
	 INNER JOIN DCMSPT.DATE_V t3 ON t17.PRPT_PROCEDURE_DATE_ID = t3.DT_DATE_ID
	 INNER JOIN DCMSPT.DATE_V t4 ON t17.PRPT_REPORT_DATE_ID = t4.DT_DATE_ID
	 LEFT JOIN panels ON t17.prpt_path_rpt_id = panels.rpt_id
	 LEFT JOIN mdx_annots impact
	 ON t17.PRPT_PATH_RPT_ID = impact.MDPRA_PRPT_PATH_RPT_ID
	 AND rtrim (impact.RESULT) = 'POSITIVE'
	 AND test_id NOT IN (25)
	 AND impact.method = panels.method
	 AND GENE <> ''
) --select * from dt

, dt2 AS (
	 SELECT
	 CASE
	 WHEN METHOD <> 'IMPACT' AND Aberration_Type = 'point_mutation' THEN Gene ||
	 CASE
	 WHEN search_hit2a <> '' THEN ' - ' || SEARCH_HIT2A
	 ELSE ''
	 END
	 WHEN METHOD <> 'IMPACT' AND Aberration_Type = 'chromosomal_rearrangement' THEN Gene || ' rearrangement'
	 WHEN dv.regEX ('(?i)' ||  replace(replace(Gene,')','\)'),'(','\(')  || ' translocation',COALESCE(SEARCH_HIT1,'') ||COALESCE(SEARCH_HIT2,''),'start') > 0 THEN Gene || ' translocation'
	 WHEN dv.regEX ('(?i)translocation',COALESCE(SEARCH_HIT1,'') ||COALESCE(SEARCH_HIT2,''),'start') > 0 THEN dv.regEXstr ('[^:]*',REPLACE(REPLACE(SEARCH_HIT2,dv.regEXstr ('\([^)]*\) ',dv.regEXstr ('(?i)(?<=NM_).*',search_hit2)),''),dv.regEXstr ('\([^)]*\) ',search_hit2),''))
	 WHEN dv.regEX ('(?i)duplication',COALESCE(SEARCH_HIT1,'') ||COALESCE(SEARCH_HIT2,''),'start') > 0 AND dv.regEX ('(?i)NM_',COALESCE(SEARCH_HIT1,'') ||COALESCE(SEARCH_HIT2,''),'matchCount') > 1 THEN dv.regEXStr ('(?i)[^ ]*',SEARCH_HIT2) || ' - ' || dv.regEXStr ('(?i)[^\W]*',REPLACE(SEARCH_HIT2,dv.regEXStr ('(?i)[^)]*\)\W*-?\W*',SEARCH_HIT2),'')) || ' - duplication'
	 WHEN dv.regEX ('(?i)duplication',COALESCE(SEARCH_HIT1,'') ||COALESCE(SEARCH_HIT2,''),'start') > 0 THEN Gene || ' - duplication'
	 WHEN dv.regEX ('(?i)reciprocal\W*inversion',COALESCE(SEARCH_HIT1,'') ||COALESCE(SEARCH_HIT2,''),'start') > 0 THEN dv.regEXstr ('[^:]*',REPLACE(REPLACE(SEARCH_HIT2,dv.regEXstr ('\([^)]*\) ',dv.regEXstr ('(?i)(?<=NM_).*',search_hit2)),''),dv.regEXstr ('\([^)]*\) ',search_hit2),''))
	 WHEN dv.regEX ('(?i)vIII deletion',COALESCE(SEARCH_HIT1,'') ||COALESCE(SEARCH_HIT2,''),'start') > 0 THEN Gene || ' - ' || 'vIII deletion'
	 WHEN dv.regEX ('(?i)splicing',COALESCE(SEARCH_HIT1,'') ||COALESCE(SEARCH_HIT2,''),'start') > 0 THEN Gene || ' - ' || 'splicing variant'
	 WHEN Aberration_Type IN ('inversion','copy number gain','copy number loss','intragenic deletion') THEN '' || Gene || ' - ' || Aberration_Type ||
	 CASE
	 WHEN Fold_Change IS NOT NULL THEN ' (' || Fold_Change || ')'
	 ELSE ''
	 END
	 WHEN method = 'IMPACT' AND dv.regEX ('(?i)p\.',COALESCE(SEARCH_HIT1,'') ||COALESCE(SEARCH_HIT2,''),'start') = 0 THEN '' || Gene || ' - ' || dv.regEXstr ('(?i)\(c\.[^\)]*\)',COALESCE(SEARCH_HIT1,'') ||COALESCE(SEARCH_HIT2,''))
	 ELSE '' || Gene ||
	 CASE
	 WHEN search_hit2a <> '' AND search_hit2a <> '12b' THEN ' - ' || SEARCH_HIT2A
	 ELSE ' - ' || Aberration_Type
	 END
	 END AS CustomGene,
	 CASE
	 WHEN Fold_Change IS NULL THEN 0
	 ELSE 1
	 END AS Fold_Change_Ind,dt.*
	 FROM dt
) --select * from dt2


, aggAberr_pre AS (
	 SELECT P_ID,
	 PATH_TYPE_ROLLUP ReportType,
	 PRPT_REPORT_TYPE PathReportTypeCD,
	 PRPTLK_PRPT_TYPE_DESC PathReportType,
	 GEN_PATH_DATE PathProcedureDate,
	 dt2.PRPT_PATH_RPT_ID,
	 TRIM(dt2.PRPT_ACCESSION_NO) AccessionNumber,
	 COALESCE(METHOD,'') DMPTestMethod,
	 COALESCE('[' || CAST(COUNT(CUSTOMGENE) AS VARCHAR(10)) || '] ' || METHOD || chr (13) || chr (10) || listagg (
	cast(CUSTOMGENE as varchar(9000))
	|| chr (13) || chr (10),'') within GROUP (ORDER BY customgene),'') Aberrations,
	 COUNT(CUSTOMGENE) AS AberrationCount
	 FROM dt2
	 GROUP BY P_ID,
	 PATH_TYPE_ROLLUP,
	 GEN_PATH_DATE,
	 PRPT_REPORT_TYPE,
	 PRPTLK_PRPT_TYPE_DESC,
	 dt2.PRPT_PATH_RPT_ID,
	 dt2.PRPT_ACCESSION_NO,
	 METHOD
) --select * from aggAberr_pre

, aggAberr AS (
	 SELECT p_id,
	 ReportType,
	 COALESCE(PathReportType,CASE WHEN PathReportTypeCD = 'F_FLOW' THEN 'Flow Cytometry' ELSE PathReportTypeCD END) PathReportType,
	 PathProcedureDate,
	 PRPT_PATH_RPT_ID,
	 AccessionNumber,
	 listagg(CASE WHEN Aberrations <> '' THEN chr (13) || chr (10) || Aberrations || chr (13) || chr (10) ELSE '' END,'') within GROUP (ORDER BY DMPTestMethod) Aberrations,
	 SUM(AberrationCount) AberrationCount
	 FROM aggAberr_pre
	 GROUP BY p_id,
	 ReportType,
	 PathReportType,
	 PathProcedureDate,
	 PRPT_PATH_RPT_ID,
	 AccessionNumber,
	 CASE
	 WHEN PathReportTypeCD = 'F_FLOW' THEN 'Flow Cytometry'
	 ELSE PathReportTypeCD
 	END
)-- select * from aggAberr



SELECT
 a.p_id,
 reporttype "Report Type",
 PathReportType "Path Report Type",
 PathProcedureDate "Path Procedure Date",
 a.PRPT_PATH_RPT_ID,
 AccessionNumber "Accession Number",
 Aberrations "Aberrations",
 AberrationCount "Aberration Count",

 COALESCE(Associated_Reports,'') "Associated Reports",
 PDRX_DMP_PATIENT_ID,
 COALESCE(PDRX_DMP_SAMPLE_ID,'') PDRX_DMP_SAMPLE_ID
 , CASE
 WHEN length(rpt.PRPT_REPORT) <= 15000 THEN rtrim (CAST(substr (rpt.PRPT_REPORT,1,15000) AS VARCHAR(15000)))
 ELSE CAST(substr (rpt.PRPT_REPORT,1,15000) AS VARCHAR(15000))
 END "path_prpt_p1",
 CASE
 WHEN length(rpt.PRPT_REPORT) > 15000 THEN rtrim (CAST(substr (rpt.PRPT_REPORT,15001,15000) AS VARCHAR(15000)))
 ELSE ''
 END "path_prpt_p2",
 CASE
 WHEN UPPER(rtrim (PRPTLK_PRPT_TYPE_DESC)) LIKE '%SURGICAL%' THEN dv.regExStr ('(?i)Specimens Submitted:' || chr (13) || chr (10) || '[\w\W]*?(?=\r\nDIAGNOSIS:|\r\n[ ]*?DIAGNOSIS:)',prpt_report)
 WHEN UPPER(rtrim (PRPTLK_PRPT_TYPE_DESC)) LIKE '%DIAGNOSTIC MOLECULAR%' THEN dv.regExStr ('(?i)Specimens Submitted:' || chr (13) || chr (10) || '[\w\W]*?(?=\r\n\r\n)',prpt_report)
 WHEN UPPER(rtrim (PRPTLK_PRPT_TYPE_DESC)) LIKE '%CYTOLOGY%' THEN dv.regExStr ('(?i)Specimen Description:' || chr (13) || chr (10) || '[\w\W]*?(?=\r\n(CYTOLOGIC)? DIAGNOSIS:)',prpt_report)
 WHEN UPPER(rtrim (PRPTLK_PRPT_TYPE_DESC)) LIKE '%CYTOGENETICS%' AND dv.regExStr ('(?i)Specimens Submitted:' || chr (13) || chr (10) || '[\w\W]*?(?=\r\n\r\n)',prpt_report) <> '' THEN dv.regExStr ('(?i)Specimens Submitted:' || chr (13) || chr (10) || '[\w\W]*?(?=\r\n\r\n)',prpt_report)
 WHEN UPPER(rtrim (PRPTLK_PRPT_TYPE_DESC)) LIKE '%CYTOGENETICS%' AND dv.regExStr ('(?i)Specimens Submitted:' || chr (13) || chr (10) || '[\w\W]*?(?=\r\n\r\n)',prpt_report) = '' THEN dv.regExStr ('(?i)Specimens Submitted: *' || chr (13) || chr (10) || '[\w\W]*?(?=\r\n)',prpt_report)
 ELSE ''
 END Spec_sub_pre
FROM aggaberr a
 JOIN path_union rpt ON a.prpt_path_rpt_id = rpt.prpt_path_rpt_id
 LEFT JOIN DCMSPT.PATH_RPRT_TYPE_LOOKUP t2 ON rpt.PRPT_REPORT_TYPE = t2.PRPTLK_PRPT_TYPE_CD
 LEFT JOIN path_refs ref ON rpt.prpt_path_rpt_id = ref.PRPTR_PATH_RPT_ID
 LEFT JOIN dmp_sample_id ON rpt.prpt_path_rpt_id = PDRX_PATH_RPT_ID
ORDER BY PathProcedureDate DESC
;



