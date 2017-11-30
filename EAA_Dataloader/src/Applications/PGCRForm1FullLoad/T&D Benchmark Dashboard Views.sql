/*
Data de-depulication views

*/
DROP VIEW IF EXISTS pgcr_dev.form1_full_vw_f1_27_elc_op_mnt_expn_deduplicated CASCADE;
CREATE VIEW pgcr_dev.form1_full_vw_f1_27_elc_op_mnt_expn_deduplicated 
AS
SELECT T.respondent
       ,T.report_yea
       ,T.spplmnt_nu
       ,T.row_number1
       ,T.row_seq
       ,T.row_prvlg
       ,T.crnt_yr_am
       ,T.prev_yr_am
       ,T.crnt_yr_a2
       ,T.prev_yr_a2
       ,T.report_prd
FROM (SELECT ROW_NUMBER() OVER (PARTITION BY respondent,report_yea,report_prd,row_number1 ORDER BY crnt_yr_am DESC,prev_yr_am DESC,crnt_yr_a2 DESC,prev_yr_a2 DESC,report_prd DESC) AS rowidentifier
             ,respondent
             ,report_yea
             ,spplmnt_nu
             ,row_number1
             ,row_seq
             ,row_prvlg
             ,crnt_yr_am
             ,prev_yr_am
             ,crnt_yr_a2
             ,prev_yr_a2
             ,report_prd
      FROM pgcr_prod.form1_full_f1_27_elc_op_mnt_expn
      WHERE report_yea>=2003) AS T
WHERE T.rowidentifier = 1;


DROP VIEW IF EXISTS pgcr_dev.form1_full_vw_f1_25_elctrc_oper_rev_deduplicated CASCADE;
CREATE VIEW pgcr_dev.form1_full_vw_f1_25_elctrc_oper_rev_deduplicated 
AS
SELECT T.respondent
       ,T.report_yea
       ,T.spplmnt_nu
       ,T.row_number1
       ,T.row_seq
       ,T.row_prvlg
       ,T.acct_dsc
       ,T.rev_amt_cr
       ,T.rev_amt_pr
       ,T.mwh_sold_c
       ,T.mwh_sold_p
       ,T.avg_cstmr_
       ,T.avg_cstmr2
       ,T.acct_dsc_f
       ,T.rev_amt_c2
       ,T.rev_amt_p2
       ,T.mwh_sold_2
       ,T.mwh_sold_3
       ,T.avg_cstmr3
       ,T.avg_cstmr4
       ,T.report_prd
FROM (SELECT ROW_NUMBER() OVER (PARTITION BY respondent,report_yea,report_prd, row_number1 ORDER BY rev_amt_cr,rev_amt_pr,mwh_sold_c,mwh_sold_p,avg_cstmr_,avg_cstmr2,acct_dsc_f,rev_amt_c2,rev_amt_p2,mwh_sold_2,mwh_sold_3,avg_cstmr3,avg_cstmr4) AS rowidentifier
             ,respondent
             ,report_yea
             ,spplmnt_nu
             ,row_number1
             ,row_seq
             ,row_prvlg
             ,acct_dsc
             ,rev_amt_cr
             ,rev_amt_pr
             ,mwh_sold_c
             ,mwh_sold_p
             ,avg_cstmr_
             ,avg_cstmr2
             ,acct_dsc_f
             ,rev_amt_c2
             ,rev_amt_p2
             ,mwh_sold_2
             ,mwh_sold_3
             ,avg_cstmr3
             ,avg_cstmr4
             ,report_prd
      FROM pgcr_prod.form1_full_f1_25_elctrc_oper_rev
      WHERE report_yea>=2003) AS T
WHERE T.rowidentifier = 1;


DROP VIEW IF EXISTS pgcr_dev.form1_full_vw_f1_65_slry_wg_dstrbtn_deduplicated CASCADE;
CREATE VIEW pgcr_dev.form1_full_vw_f1_65_slry_wg_dstrbtn_deduplicated 
AS
SELECT respondent
       ,report_yea
       ,spplmnt_nu
       ,row_number1
       ,row_seq
       ,row_prvlg
       ,classifica
       ,drct_pyrl_
       ,alloc_of_p
       ,total
       ,classific2
       ,drct_pyrl2
       ,alloc_of_2
       ,total_f
       ,report_prd
FROM (SELECT ROW_NUMBER() OVER (PARTITION BY respondent,report_yea,report_prd,row_number1 ORDER BY respondent DESC,report_yea DESC,spplmnt_nu DESC,row_number1 DESC,row_seq DESC,row_prvlg DESC,classifica DESC,drct_pyrl_ DESC,alloc_of_p DESC,total DESC,classific2 DESC,drct_pyrl2 DESC,alloc_of_2 DESC,total_f DESC,report_prd DESC) AS rowidentifier
             ,respondent
             ,report_yea
             ,spplmnt_nu
             ,row_number1
             ,row_seq
             ,row_prvlg
             ,classifica
             ,drct_pyrl_
             ,alloc_of_p
             ,total
             ,classific2
             ,drct_pyrl2
             ,alloc_of_2
             ,total_f
             ,report_prd
      FROM pgcr_prod.form1_full_f1_65_slry_wg_dstrbtn
      WHERE report_yea>=2003) AS O
WHERE O.rowidentifier = 1;







/*
End of Data de-depulication views
*/



/*
Name of the datasource in Roger's dashboard: "form1_f1_52+ (Multiple Connections)"
Name of the Tableau Worksheet: "T&D CAPEX"
*/
DROP VIEW IF EXISTS pgcr_dev.form1_full_vw_f1_52_capex_book CASCADE;
CREATE VIEW pgcr_dev.form1_full_vw_f1_52_capex_book 
AS
SELECT 
        respondent
       ,row_number1
       ,AVG(capex_by_book) AS avg_capex_by_book
       ,STDDEV(capex_by_book) AS stddev_capex_by_book
FROM
(       
    SELECT respondent
           ,row_number1
           ,CASE WHEN begin_yr_b=0 THEN 0 ELSE addition/begin_yr_b END AS capex_by_book
    FROM pgcr_prod.form1_full_f1_52_plant_in_srvce
    WHERE report_yea>=2003
) AS O
GROUP BY respondent
       ,row_number1;
       

DROP VIEW IF EXISTS pgcr_dev.form1_full_vw_plant_in_service CASCADE;
CREATE VIEW pgcr_dev.form1_full_vw_plant_in_service 
AS
SELECT F1_1.responden2    AS F1_1_responden2
       ,B.description     AS F1_52_description
       ,F1_52.respondent  AS F1_52_respondent
       ,F1_52.report_yea  AS F1_52_report_yea
       ,F1_52.spplmnt_nu  AS F1_52_spplmnt_nu
       ,F1_52.row_number1 AS F1_52_row_number1
       ,F1_52.row_seq     AS F1_52_row_seq
       ,F1_52.row_prvlg   AS F1_52_row_prvlg
       ,F1_52.begin_yr_b  AS F1_52_begin_yr_b
       ,F1_52.addition    AS F1_52_addition
       ,F1_52.retirement  AS F1_52_retirement
       ,F1_52.adjustment  AS F1_52_adjustment
       ,F1_52.transfers   AS F1_52_transfers
       ,F1_52.yr_end_bal  AS F1_52_yr_end_bal
       ,F1_52.begin_yr_2  AS F1_52_begin_yr_2
       ,F1_52.addition_f  AS F1_52_addition_f
       ,F1_52.retiremen2  AS F1_52_retiremen2
       ,F1_52.adjustmen2  AS F1_52_adjustmen2
       ,F1_52.transfers_  AS F1_52_transfers_
       ,F1_52.yr_end_ba2  AS F1_52_yr_end_ba2
       ,F1_52.report_prd  AS F1_52_report_prd
       ,case 
			when ABS(f1_52_begin_yr_b-AVG(f1_52_begin_yr_b) OVER (PARTITION BY f1_52_respondent,f1_52_row_number1)) > 3*STDDEV(f1_52_begin_yr_b) OVER (PARTITION BY f1_52_respondent,f1_52_row_number1) THEN 1       				
            ELSE 0
        END AS is_outlier_f1_52_begin_yr_b
        ,case 
			when ABS(F1_52_addition-AVG(F1_52_addition) OVER (PARTITION BY f1_52_respondent,f1_52_row_number1)) > 3*STDDEV(F1_52_addition) OVER (PARTITION BY f1_52_respondent,f1_52_row_number1) THEN 1       				
            ELSE 0
        END AS is_outlier_F1_52_addition
        ,AVG(f1_52_begin_yr_b) OVER (PARTITION BY f1_52_respondent,f1_52_row_number1) as avg_f1_52_begin_yr_b
        ,STDDEV(f1_52_begin_yr_b) OVER (PARTITION BY f1_52_respondent,f1_52_row_number1) as stddev_f1_52_begin_yr_b
        ,AVG(F1_52_addition) OVER (PARTITION BY f1_52_respondent,f1_52_row_number1) as avg_F1_52_addition
        ,STDDEV(F1_52_addition) OVER (PARTITION BY f1_52_respondent,f1_52_row_number1) as stddev_F1_52_addition
        ,F1_52_capex_book.avg_capex_by_book
        ,F1_52_capex_book.stddev_capex_by_book
FROM pgcr_prod.form1_full_f1_52_plant_in_srvce F1_52
  JOIN pgcr_prod.form1_full_f1_1_respondent_id AS F1_1
    ON F1_52.respondent = F1_1.respondent
   AND F1_52.report_yea = F1_1.report_yea
  JOIN pgcr_prod.ihsmarkitdata_electric_plant_in_service_row_map B
    ON F1_52.row_number1 = B.row_number
  JOIN pgcr_dev.form1_full_vw_f1_52_capex_book as F1_52_capex_book
    ON F1_52_capex_book.respondent = F1_52.respondent
   AND F1_52_capex_book.row_number1=F1_52.row_number1
WHERE F1_52.report_prd = 12
    AND F1_52.report_yea>=2003;



/*
Name of the datasource in Roger's dashboard: "form1_f1_27 (pgcr_prod.form1_f1_27)+ (pgcr_prod)"
Name of the Tableau Worksheet: "Distribution O&M Sum"
*/
DROP VIEW IF EXISTS pgcr_dev.form1_full_vw_F1_27_F1_52_F1_1 CASCADE;
CREATE VIEW pgcr_dev.form1_full_vw_F1_27_F1_52_F1_1 
AS
SELECT F1_1.responden2    AS F1_1_responden2
       ,OM.description    AS F1_27_description
       ,F1_27.respondent  AS F1_27_respondent
       ,F1_27.report_yea  AS F1_27_report_yea
       ,F1_27.row_number1 AS F1_27_row_number1
       ,F1_27.spplmnt_nu  AS F1_27_spplmnt_nu
       ,F1_27.crnt_yr_am  AS F1_27_crnt_yr_am
       ,F1_27.prev_yr_am  AS F1_27_prev_yr_am
       ,F1_27.crnt_yr_a2  AS F1_27_crnt_yr_a2
       ,F1_27.prev_yr_a2  AS F1_27_prev_yr_a2
       ,F1_27.report_prd  AS F1_27_report_prd
       ,SM.description    AS F1_52_description
       ,F1_52.row_number1 AS F1_52_row_number1
       ,F1_52.begin_yr_b  AS F1_52_begin_yr_b
       ,F1_52.addition    AS F1_52_addition
       ,F1_52.retirement  AS F1_52_retirement
       ,F1_52.adjustment  AS F1_52_adjustment
       ,F1_52.transfers   AS F1_52_transfers
       ,F1_52.yr_end_bal  AS F1_52_yr_end_bal
       ,F1_52.begin_yr_2  AS F1_52_begin_yr_2
       ,F1_52.addition_f  AS F1_52_addition_f
       ,F1_52.retiremen2  AS F1_52_retiremen2
       ,F1_52.adjustmen2  AS F1_52_adjustmen2
       ,F1_52.transfers_  AS F1_52_transfers_
       ,F1_52.yr_end_ba2  AS F1_52_yr_end_ba2
       ,case 
			when ABS(f1_52_begin_yr_b-AVG(f1_52_begin_yr_b) OVER (PARTITION BY F1_27_respondent,F1_52_row_number1)) > 3*STDDEV(f1_52_begin_yr_b) OVER (PARTITION BY F1_27_respondent,F1_52_row_number1) THEN 1       				
            ELSE 0
        END AS is_outlier_f1_52_begin_yr_b
        ,case 
			when ABS(F1_27_crnt_yr_am-AVG(F1_27_crnt_yr_am) OVER (PARTITION BY F1_27_respondent,F1_52_row_number1)) > 3*STDDEV(F1_27_crnt_yr_am) OVER (PARTITION BY F1_27_respondent,F1_52_row_number1) THEN 1       				
            ELSE 0
        END AS is_outlier_F1_27_crnt_yr_am
FROM pgcr_dev.form1_full_vw_f1_27_elc_op_mnt_expn_deduplicated AS F1_27
  JOIN pgcr_prod.ihsmarkitdata_o_and_m_row_map OM
    ON F1_27.row_number1 = OM.row_number
  JOIN pgcr_prod.form1_full_f1_52_plant_in_srvce F1_52
    ON F1_27.respondent = F1_52.respondent
   AND F1_27.report_yea = F1_52.report_yea
   AND F1_27.report_prd = F1_52.report_prd
  JOIN pgcr_prod.ihsmarkitdata_electric_plant_in_service_row_map SM
    ON F1_52.row_number1 = SM.row_number
  JOIN pgcr_prod.form1_full_f1_1_respondent_id AS F1_1
    ON F1_27.respondent = F1_1.respondent
   AND F1_27.report_yea = F1_1.report_yea
WHERE F1_27.report_prd = 12
    AND F1_27.report_yea>=2003;

GRANT SELECT ON pgcr_dev.form1_full_vw_F1_27_F1_52_F1_1 TO GROUP analysts;
GRANT SELECT ON pgcr_dev.form1_full_vw_F1_27_F1_52_F1_1 TO GROUP pgcr_readonly;
  
  

/*
Tableau view: Distribution O&M Sum per MWh
*/
DROP VIEW IF EXISTS pgcr_dev.form1_full_vw_F1_27_F1_25_F1_52_F1_1 CASCADE;
CREATE VIEW pgcr_dev.form1_full_vw_F1_27_F1_25_F1_52_F1_1 
AS 
SELECT 
    F1_1.responden2    AS F1_1_responden2
    ,OM.description    AS F1_27_description
    ,SM.description    AS F_52_description
    ,F1_27.respondent  AS F1_27_respondent
    ,F1_27.report_yea  AS F1_27_report_yea
    ,F1_27.spplmnt_nu  AS F1_27_spplmnt_nu
    ,F1_27.row_number1 AS F1_27_row_number1
    ,F1_27.row_seq     AS F1_27_row_seq
    ,F1_27.row_prvlg   AS F1_27_row_prvlg
    ,F1_27.crnt_yr_am  AS F1_27_crnt_yr_am
    ,F1_27.prev_yr_am  AS F1_27_prev_yr_am
    ,F1_27.crnt_yr_a2  AS F1_27_crnt_yr_a2
    ,F1_27.prev_yr_a2  AS F1_27_prev_yr_a2
    ,F1_27.report_prd  AS F1_27_report_prd
    ,F1_25.respondent  AS F1_25_respondent
    ,F1_25.report_yea  AS F1_25_report_yea
    ,F1_25.spplmnt_nu  AS F1_25_spplmnt_nu
    ,F1_25.row_number1 AS F1_25_row_number1
    ,F1_25.row_seq     AS F1_25_row_seq
    ,F1_25.row_prvlg   AS F1_25_row_prvlg
    ,F1_25.acct_dsc    AS F1_25_acct_dsc
    ,F1_25.rev_amt_cr  AS F1_25_rev_amt_cr
    ,F1_25.rev_amt_pr  AS F1_25_rev_amt_pr
    ,F1_25.mwh_sold_c  AS F1_25_mwh_sold_c
    ,F1_25.mwh_sold_p  AS F1_25_mwh_sold_p
    ,F1_25.avg_cstmr_  AS F1_25_avg_cstmr_
    ,F1_25.avg_cstmr2  AS F1_25_avg_cstmr2
    ,F1_25.acct_dsc_f  AS F1_25_acct_dsc_f
    ,F1_25.rev_amt_c2  AS F1_25_rev_amt_c2
    ,F1_25.rev_amt_p2  AS F1_25_rev_amt_p2
    ,F1_25.mwh_sold_2  AS F1_25_mwh_sold_2
    ,F1_25.mwh_sold_3  AS F1_25_mwh_sold_3
    ,F1_25.avg_cstmr3  AS F1_25_avg_cstmr3
    ,F1_25.avg_cstmr4  AS F1_25_avg_cstmr4
    ,F1_25.report_prd  AS F1_25_report_prd
    ,F1_52.respondent  AS F1_52_respondent
    ,F1_52.report_yea  AS F1_52_report_yea
    ,F1_52.spplmnt_nu  AS F1_52_spplmnt_nu
    ,F1_52.row_number1 AS F1_52_row_number1
    ,F1_52.row_seq     AS F1_52_row_seq
    ,F1_52.row_prvlg   AS F1_52_row_prvlg
    ,F1_52.begin_yr_b  AS F1_52_begin_yr_b
    ,F1_52.addition    AS F1_52_addition
    ,F1_52.retirement  AS F1_52_retirement
    ,F1_52.adjustment  AS F1_52_adjustment
    ,F1_52.transfers   AS F1_52_transfers
    ,F1_52.yr_end_bal  AS F1_52_yr_end_bal
    ,F1_52.begin_yr_2  AS F1_52_begin_yr_2
    ,F1_52.addition_f  AS F1_52_addition_f
    ,F1_52.retiremen2  AS F1_52_retiremen2
    ,F1_52.adjustmen2  AS F1_52_adjustmen2
    ,F1_52.transfers_  AS F1_52_transfers_
    ,F1_52.yr_end_ba2  AS F1_52_yr_end_ba2
    ,F1_52.report_prd  AS F1_52_report_prd
    ,case 
        when ABS(F1_27_crnt_yr_am-AVG(F1_27_crnt_yr_am) OVER (PARTITION BY F1_27_respondent,F1_52_row_number1)) > 3*STDDEV(F1_27_crnt_yr_am) OVER (PARTITION BY F1_27_respondent,F1_52_row_number1) THEN 1       				
        ELSE 0
    END AS is_outlier_F1_27_crnt_yr_am
    ,case 
        when ABS(F1_25_mwh_sold_c-AVG(F1_25_mwh_sold_c) OVER (PARTITION BY F1_27_respondent,F1_52_row_number1)) > 3*STDDEV(F1_25_mwh_sold_c) OVER (PARTITION BY F1_27_respondent,F1_52_row_number1) THEN 1       				
        ELSE 0
    END AS is_outlier_F1_25_mwh_sold_c    
FROM pgcr_dev.form1_full_vw_f1_27_elc_op_mnt_expn_deduplicated AS F1_27
JOIN pgcr_prod.ihsmarkitdata_o_and_m_row_map OM
    ON F1_27.row_number1 = OM.row_number
JOIN pgcr_prod.form1_full_f1_25_elctrc_oper_rev F1_25
    ON F1_27.respondent  = F1_25.respondent
    AND F1_27.report_yea = F1_25.report_yea
    AND F1_27.report_prd = F1_25.report_prd
JOIN pgcr_prod.form1_full_f1_52_plant_in_srvce F1_52
    ON F1_52.respondent  = F1_25.respondent
    AND F1_52.report_yea = F1_25.report_yea
    AND F1_52.report_prd = F1_25.report_prd
JOIN pgcr_prod.ihsmarkitdata_electric_plant_in_service_row_map SM
    ON F1_52.row_number1 = SM.row_number
JOIN pgcr_prod.form1_full_f1_1_respondent_id AS F1_1
    ON F1_25.respondent  = F1_1.respondent
    AND F1_25.report_yea = F1_1.report_yea
WHERE F1_25.report_prd = 12
    AND F1_27.report_yea>=2003
    AND   F1_25.row_number1 = 10;

GRANT SELECT ON pgcr_dev.form1_full_vw_F1_27_F1_25_F1_52_F1_1 TO GROUP analysts;
GRANT SELECT ON pgcr_dev.form1_full_vw_F1_27_F1_25_F1_52_F1_1 TO GROUP pgcr_readonly;
  
  

/*
Name of the datasource in Roger's dashboard: "form1_f1_25 (pgcr_prod.form1_f1_25)+ (pgcr_prod)"
Name of the Tableau Worksheet: "Cust and Revenue"
Simple join b/w F1_25 & F1_1
The where conditions will be tableau filters
*/
DROP VIEW IF EXISTS pgcr_dev.form1_full_vw_F1_25_F1_1 CASCADE;
CREATE VIEW pgcr_dev.form1_full_vw_F1_25_F1_1 
AS
SELECT 
    F1_1.responden2    AS F1_1_responden2
    ,F1_25.respondent  AS F1_25_respondent
    ,F1_25.report_yea  AS F1_25_report_yea
    ,F1_25.spplmnt_nu  AS F1_25_spplmnt_nu
    ,F1_25.row_number1 AS F1_25_row_number1
    ,F1_25.row_seq     AS F1_25_row_seq
    ,F1_25.row_prvlg   AS F1_25_row_prvlg
    ,F1_25.acct_dsc    AS F1_25_acct_dsc
    ,F1_25.rev_amt_cr  AS F1_25_rev_amt_cr
    ,F1_25.rev_amt_pr  AS F1_25_rev_amt_pr
    ,F1_25.mwh_sold_c  AS F1_25_mwh_sold_c
    ,F1_25.mwh_sold_p  AS F1_25_mwh_sold_p
    ,F1_25.avg_cstmr_  AS F1_25_avg_cstmr_
    ,F1_25.avg_cstmr2  AS F1_25_avg_cstmr2
    ,F1_25.acct_dsc_f  AS F1_25_acct_dsc_f
    ,F1_25.rev_amt_c2  AS F1_25_rev_amt_c2
    ,F1_25.rev_amt_p2  AS F1_25_rev_amt_p2
    ,F1_25.mwh_sold_2  AS F1_25_mwh_sold_2
    ,F1_25.mwh_sold_3  AS F1_25_mwh_sold_3
    ,F1_25.avg_cstmr3  AS F1_25_avg_cstmr3
    ,F1_25.avg_cstmr4  AS F1_25_avg_cstmr4
    ,F1_25.report_prd  AS F1_25_report_prd
FROM pgcr_dev.form1_full_vw_f1_25_elctrc_oper_rev_deduplicated AS F1_25
JOIN pgcr_prod.form1_full_f1_1_respondent_id AS F1_1
ON F1_25.respondent = F1_1.respondent
    AND F1_25.report_yea = F1_1.report_yea
    AND F1_25.report_yea>=2003
WHERE F1_25.report_prd = 12;

    
/*
Name of the datasource in Roger's dashboard: "form1_f1_21 (pgcr_prod.form1_f1_21) (pgcr_prod)"
Name of the Tableau Worksheet: "Distribution Deprec & Amort"
The where conditions will be tableau filters
uses row_number1=7
*/
DROP VIEW IF EXISTS pgcr_dev.form1_full_vw_F1_21_F1_52_F1_1 CASCADE;
CREATE VIEW pgcr_dev.form1_full_vw_F1_21_F1_52_F1_1
AS
SELECT
    F1_1.responden2    AS F1_1_responden2
    ,SM.description    AS F1_52_description
    ,F1_21.respondent  AS F1_21_respondent
    ,F1_21.report_yea  AS F1_21_report_yea
    ,F1_21.spplmnt_nu  AS F1_21_spplmnt_nu
    ,F1_21.row_number1 AS F1_21_row_number1
    ,F1_21.row_seq     AS F1_21_row_seq
    ,F1_21.row_prvlg   AS F1_21_row_prvlg
    ,F1_21.depr_expn   AS F1_21_depr_expn
    ,F1_21.depr_asset  AS F1_21_depr_asset
    ,F1_21.limterm_el  AS F1_21_limterm_el
    ,F1_21.othr_elc_p  AS F1_21_othr_elc_p
    ,F1_21.total       AS F1_21_total
    ,F1_21.depr_expn_  AS F1_21_depr_expn_
    ,F1_21.depr_asse2  AS F1_21_depr_asse2
    ,F1_21.limterm_e2  AS F1_21_limterm_e2
    ,F1_21.othr_elc_2  AS F1_21_othr_elc_2
    ,F1_21.total_f     AS F1_21_total_f
    ,F1_21.report_prd  AS F1_21_report_prd
    ,F1_52.row_number1 AS F1_52_row_number1
    ,F1_52.row_seq     AS F1_52_row_seq
    ,F1_52.row_prvlg   AS F1_52_row_prvlg
    ,F1_52.begin_yr_b  AS F1_52_begin_yr_b
    ,F1_52.addition    AS F1_52_addition
    ,F1_52.retirement  AS F1_52_retirement
    ,F1_52.adjustment  AS F1_52_adjustment
    ,F1_52.transfers   AS F1_52_transfers
    ,F1_52.yr_end_bal  AS F1_52_yr_end_bal
    ,F1_52.begin_yr_2  AS F1_52_begin_yr_2
    ,F1_52.addition_f  AS F1_52_addition_f
    ,F1_52.retiremen2  AS F1_52_retiremen2
    ,F1_52.adjustmen2  AS F1_52_adjustmen2
    ,F1_52.transfers_  AS F1_52_transfers_
    ,F1_52.yr_end_ba2  AS F1_52_yr_end_ba2
    ,case 
        when ABS(F1_21_total-AVG(F1_21_total) OVER (PARTITION BY F1_21_respondent,F1_52_row_number1)) > 3*STDDEV(F1_21_total) OVER (PARTITION BY F1_21_respondent,F1_52_row_number1) THEN 1       				
        ELSE 0
    END AS is_outlier_F1_21_total
    ,case 
        when ABS(F1_52_begin_yr_b-AVG(F1_52_begin_yr_b) OVER (PARTITION BY F1_21_respondent,F1_52_row_number1)) > 3*STDDEV(F1_52_begin_yr_b) OVER (PARTITION BY F1_21_respondent,F1_52_row_number1) THEN 1       				
        ELSE 0
    END AS is_outlier_F1_52_begin_yr_b    
FROM pgcr_prod.form1_full_f1_21_dacs_epda    AS F1_21
JOIN pgcr_prod.form1_full_f1_1_respondent_id AS F1_1
    ON F1_21.respondent = F1_1.respondent
    AND F1_21.report_yea = F1_1.report_yea
JOIN pgcr_prod.form1_full_f1_52_plant_in_srvce AS F1_52
    ON  F1_52.respondent = F1_21.respondent
    AND F1_52.report_yea = F1_21.report_yea
    AND F1_52.report_prd = F1_21.report_prd
JOIN pgcr_prod.ihsmarkitdata_electric_plant_in_service_row_map SM
    ON F1_52.row_number1 = SM.row_number
WHERE F1_21.report_prd = 12
    AND F1_21.report_yea>=2003;

GRANT SELECT ON pgcr_dev.form1_full_vw_F1_21_F1_52_F1_1 TO GROUP analysts;
GRANT SELECT ON pgcr_dev.form1_full_vw_F1_21_F1_52_F1_1 TO GROUP pgcr_readonly;
  


/*
Name of the datasource in Roger's dashboard: "form1_f1_65 (pgcr_prod.form1_f1_65)+ (pgcr_prod)"
Name of the Tableau Worksheet: "Transmission Operations"
The where conditions will be tableau filters
*/
DROP VIEW IF EXISTS pgcr_dev.form1_full_vw_F1_65_F1_27_F1_1 CASCADE;
CREATE VIEW pgcr_dev.form1_full_vw_F1_65_F1_27_F1_1
AS 
SELECT
    F1_1.responden2    AS F1_1_responden2
    ,PM.description    AS F1_65_description
    ,OM.description    AS F1_27_description
    ,F1_65.respondent  AS F1_65_respondent
    ,F1_65.report_yea  AS F1_65_report_yea
    ,F1_65.spplmnt_nu  AS F1_65_spplmnt_nu
    ,F1_65.row_number1 AS F1_65_row_number1
    ,F1_65.row_seq     AS F1_65_row_seq
    ,F1_65.row_prvlg   AS F1_65_row_prvlg
    ,F1_65.classifica  AS F1_65_classifica
    ,F1_65.drct_pyrl_  AS F1_65_drct_pyrl_
    ,F1_65.alloc_of_p  AS F1_65_alloc_of_p
    ,F1_65.total       AS F1_65_total
    ,F1_65.classific2  AS F1_65_classific2
    ,F1_65.drct_pyrl2  AS F1_65_drct_pyrl2
    ,F1_65.alloc_of_2  AS F1_65_alloc_of_2
    ,F1_65.total_f     AS F1_65_total_f
    ,F1_65.report_prd  AS F1_65_report_prd
    ,F1_27.respondent  AS F1_27_respondent
    ,F1_27.report_yea  AS F1_27_report_yea
    ,F1_27.spplmnt_nu  AS F1_27_spplmnt_nu
    ,F1_27.row_number1 AS F1_27_row_number1
    ,F1_27.row_seq     AS F1_27_row_seq
    ,F1_27.row_prvlg   AS F1_27_row_prvlg
    ,F1_27.crnt_yr_am  AS F1_27_crnt_yr_am
    ,F1_27.prev_yr_am  AS F1_27_prev_yr_am
    ,F1_27.crnt_yr_a2  AS F1_27_crnt_yr_a2
    ,F1_27.prev_yr_a2  AS F1_27_prev_yr_a2
    ,F1_27.report_prd  AS F1_27_report_prd
    ,case 
        when ABS(F1_65_drct_pyrl_-AVG(F1_65_drct_pyrl_) OVER (PARTITION BY F1_65_respondent,F1_65_row_number1, F1_27_row_number1)) > 3*STDDEV(F1_65_drct_pyrl_) OVER (PARTITION BY F1_65_respondent,F1_65_row_number1, F1_27_row_number1) THEN 1       				
        ELSE 0
    END AS is_outlier_F1_65_drct_pyrl_ 
    ,case 
        when ABS(F1_27_crnt_yr_am-AVG(F1_27_crnt_yr_am) OVER (PARTITION BY F1_65_respondent,F1_65_row_number1, F1_27_row_number1)) > 3*STDDEV(F1_27_crnt_yr_am) OVER (PARTITION BY F1_65_respondent,F1_65_row_number1, F1_27_row_number1) THEN 1       				
        ELSE 0
    END AS is_outlier_F1_27_crnt_yr_am
FROM pgcr_dev.form1_full_vw_f1_65_slry_wg_dstrbtn_deduplicated F1_65 
JOIN pgcr_prod.ihsmarkitdata_payroll_map PM
    ON F1_65.row_number1 = PM.row_number
JOIN pgcr_dev.form1_full_vw_f1_27_elc_op_mnt_expn_deduplicated AS F1_27
    ON  F1_65.respondent = F1_27.respondent
    AND F1_65.report_yea = F1_27.report_yea
    AND F1_65.report_prd = F1_27.report_prd
JOIN pgcr_prod.ihsmarkitdata_o_and_m_row_map OM
    ON F1_27.row_number1 = OM.row_number
JOIN pgcr_prod.form1_full_f1_1_respondent_id AS F1_1
    ON F1_65.respondent  = F1_1.respondent
    AND F1_65.report_yea = F1_1.report_yea
WHERE F1_65.report_prd   = 12
    AND F1_65.report_yea>=2003;

    
GRANT SELECT ON pgcr_dev.form1_full_vw_F1_65_F1_27_F1_1 TO GROUP analysts;
GRANT SELECT ON pgcr_dev.form1_full_vw_F1_65_F1_27_F1_1 TO GROUP pgcr_readonly;
    
    
/*
Name of the datasource in Roger's dashboard: "form1_f1_74 (pgcr_prod.form1_f1_74)+ (pgcr_prod)"
Name of the Tableau Worksheet: "Transmission Pole Miles"
Simple join b/w 2 tables: F1_74 & F1_1
The where conditions will be tableau filters
*/
DROP VIEW IF EXISTS pgcr_dev.form1_full_vw_F1_74_F1_1 CASCADE;
CREATE VIEW pgcr_dev.form1_full_vw_F1_74_F1_1
AS 
SELECT
    F1_1.responden2     AS F1_1_responden2
    ,F1_74.respondent	AS F1_74_respondent	
    ,F1_74.report_yea	AS F1_74_report_yea	
    ,F1_74.spplmnt_nu	AS F1_74_spplmnt_nu	
    ,F1_74.row_number1	AS F1_74_row_number1	
    ,F1_74.row_seq	    AS F1_74_row_seq	    
    ,F1_74.row_prvlg	AS F1_74_row_prvlg	
    ,F1_74.designatio	AS F1_74_designatio	
    ,F1_74.designati2	AS F1_74_designati2	
    ,F1_74.voltage_op	AS F1_74_voltage_op	
    ,F1_74.designed_v	AS F1_74_designed_v	
    ,F1_74.structure	AS F1_74_structure	
    ,F1_74.length_dsg	AS F1_74_length_dsg	
    ,F1_74.length_ano	AS F1_74_length_ano	
    ,F1_74.num_of_cir	AS F1_74_num_of_cir	
    ,F1_74.conductor_	AS F1_74_conductor_	
    ,F1_74.cost_land	AS F1_74_cost_land	
    ,F1_74.cost_other	AS F1_74_cost_other	
    ,F1_74.cost_total	AS F1_74_cost_total	
    ,F1_74.expns_oper	AS F1_74_expns_oper	
    ,F1_74.expns_main	AS F1_74_expns_main	
    ,F1_74.expns_rent	AS F1_74_expns_rent	
    ,F1_74.expns_tota	AS F1_74_expns_tota	
    ,F1_74.designati3	AS F1_74_designati3	
    ,F1_74.designati4	AS F1_74_designati4	
    ,F1_74.voltage_o2	AS F1_74_voltage_o2	
    ,F1_74.designed_2	AS F1_74_designed_2	
    ,F1_74.structure_	AS F1_74_structure_	
    ,F1_74.length_ds2	AS F1_74_length_ds2	
    ,F1_74.length_an2	AS F1_74_length_an2	
    ,F1_74.num_of_ci2	AS F1_74_num_of_ci2	
    ,F1_74.conductor2	AS F1_74_conductor2	
    ,F1_74.cost_land_	AS F1_74_cost_land_	
    ,F1_74.cost_othe2	AS F1_74_cost_othe2	
    ,F1_74.cost_tota2	AS F1_74_cost_tota2	
    ,F1_74.expns_ope2	AS F1_74_expns_ope2	
    ,F1_74.expns_mai2	AS F1_74_expns_mai2	
    ,F1_74.expns_ren2	AS F1_74_expns_ren2	
    ,F1_74.expns_tot2	AS F1_74_expns_tot2	
    ,F1_74.report_prd	AS F1_74_report_prd	
FROM pgcr_prod.form1_full_f1_74_xmssn_line AS F1_74
  JOIN pgcr_prod.form1_full_f1_1_respondent_id AS F1_1
    ON F1_74.respondent = F1_1.respondent
   AND F1_74.report_yea = F1_1.report_yea
WHERE F1_74.report_prd = 12
    AND F1_74.report_yea>=2003;    



DROP VIEW IF EXISTS pgcr_dev.form1_full_vw_tandd_capex_normalized CASCADE;
CREATE VIEW pgcr_dev.form1_full_vw_tandd_capex_normalized
AS
SELECT f1_1.responden2    AS f1_1_responden2
       ,b.description     AS f1_52_description
       ,f1_25.mwh_sold_c  AS f1_25_mwh_sold_c
       ,f1_25.avg_cstmr_  AS f1_25_avg_cstmr_
       ,f1_52.respondent  AS f1_52_respondent
       ,f1_52.report_yea  AS f1_52_report_yea
       ,f1_52.spplmnt_nu  AS f1_52_spplmnt_nu
       ,f1_52.row_number1 AS f1_52_row_number1
       ,f1_52.row_seq     AS f1_52_row_seq
       ,f1_52.row_prvlg   AS f1_52_row_prvlg
       ,f1_52.begin_yr_b  AS f1_52_begin_yr_b
       ,f1_52.addition    AS f1_52_addition
       ,f1_52.retirement  AS f1_52_retirement
       ,f1_52.adjustment  AS f1_52_adjustment
       ,f1_52.transfers   AS f1_52_transfers
       ,f1_52.yr_end_bal  AS f1_52_yr_end_bal
       ,f1_52.begin_yr_2  AS f1_52_begin_yr_2
       ,f1_52.addition_f  AS f1_52_addition_f
       ,f1_52.retiremen2  AS f1_52_retiremen2
       ,f1_52.adjustmen2  AS f1_52_adjustmen2
       ,f1_52.transfers_  AS f1_52_transfers_
       ,f1_52.yr_end_ba2  AS f1_52_yr_end_ba2       
       ,case 
			when ABS(f1_52_begin_yr_b-AVG(f1_52_begin_yr_b) OVER (PARTITION BY f1_52_respondent,f1_52_row_number1)) > 3*STDDEV(f1_52_begin_yr_b) OVER (PARTITION BY f1_52_respondent,f1_52_row_number1) THEN 1       				
            ELSE 0
        END AS is_outlier_f1_52_begin_yr_b
        ,case 
			when ABS(F1_52_addition-AVG(F1_52_addition) OVER (PARTITION BY f1_52_respondent,f1_52_row_number1)) > 3*STDDEV(F1_52_addition) OVER (PARTITION BY f1_52_respondent,f1_52_row_number1) THEN 1       				
            ELSE 0
        END AS is_outlier_F1_52_addition
        ,case
            WHEN ABS(f1_25_mwh_sold_c-AVG(f1_25_mwh_sold_c) OVER (PARTITION BY f1_52_respondent,f1_52_row_number1)) > 3*STDDEV(f1_25_mwh_sold_c) OVER (PARTITION BY f1_52_respondent,f1_52_row_number1) THEN 1
            ELSE 0
        end AS is_outlier_F1_25_mwh_sold_c
        ,AVG(f1_52_begin_yr_b) OVER (PARTITION BY f1_52_respondent,f1_52_row_number1) as avg_f1_52_begin_yr_b
        ,STDDEV(f1_52_begin_yr_b) OVER (PARTITION BY f1_52_respondent,f1_52_row_number1) as stddev_f1_52_begin_yr_b
        ,AVG(F1_52_addition) OVER (PARTITION BY f1_52_respondent,f1_52_row_number1) as avg_F1_52_addition
        ,STDDEV(F1_52_addition) OVER (PARTITION BY f1_52_respondent,f1_52_row_number1) as stddev_F1_52_addition
FROM pgcr_prod.form1_full_f1_52_plant_in_srvce f1_52
  JOIN pgcr_prod.form1_full_f1_1_respondent_id f1_1
    ON f1_52.respondent = f1_1.respondent
   AND f1_52.report_yea = f1_1.report_yea
  JOIN pgcr_prod.ihsmarkitdata_electric_plant_in_service_row_map b
    ON f1_52.row_number1 = b.row_number
  JOIN pgcr_prod.form1_full_f1_25_elctrc_oper_rev f1_25
    ON f1_52.respondent = f1_25.respondent
   AND f1_52.report_yea = f1_25.report_yea
   AND f1_52.report_prd = f1_25.report_prd
WHERE f1_52.report_yea >= 2003
AND   f1_25.row_number1 = 10
AND   f1_52.report_prd = 12;


GRANT SELECT ON pgcr_dev.form1_full_vw_tandd_capex_normalized TO GROUP analysts;
GRANT SELECT ON pgcr_dev.form1_full_vw_tandd_capex_normalized TO GROUP pgcr_readonly;
  
    