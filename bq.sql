with rs_sgccr AS 
(SELECT
  -- Replaced all blanks with #
  CASE WHEN bic_z_masclnt = "" THEN "#"
  ELSE bic_z_masclnt END AS `Master Client Level 1`,
  CASE WHEN mclnt_lvl1_desc = "" THEN "#"
  ELSE mclnt_lvl1_desc END AS `Master Client L1 Name`,
  ------------------------------------
  market_desc `Market`, 

  -- Replaced all blanks with "Not Assigned"  but note: 9940470345, 9940482700
  CASE WHEN clnt_pot_hist_desc = "" THEN "Not Assigned"
  ELSE clnt_pot_hist_desc END AS `Client Classification`,

  -- Replaced all blanks with #
  CASE WHEN bic_z_mascont = "" THEN "#"
  ELSE bic_z_mascont END AS `Master Contract`,
  CASE WHEN mast_cntrct_desc = "" THEN "#"
  ELSE mast_cntrct_desc END AS `Master Contract Name`,
  CASE WHEN bic_z_contrct = "" THEN "#"
  ELSE bic_z_contrct END AS`Contract`,
  CASE WHEN cntrct_num_desc = "" THEN "#"
  ELSE cntrct_num_desc END AS `Contract Name`,
  ------------------------------------

  marketunit_desc `Market Unit`,
  country_loc_desc `Country_Location`,
  clientgroup_desc `Client Group`,
  sg_md `Service Group`,
  zindtry_desc `Industry`,
  industry_group_lgtext `Industry Group`,

  --Aggregated Rev metrics
  SUM(revenue) `Revenue`,
  SUM(servicesrevenue) `Services Revenue`,
  SUM(z_revshrng) `Revenue Sharing`,

    -- Added FYQ column
  CASE 
  WHEN left(fiscper,3) IN ("001", "002", "003") THEN CONCAT("FY", RIGHT(fiscper,2)," Q1")
  WHEN left(fiscper,3) IN ("004", "005", "006") THEN CONCAT("FY", RIGHT(fiscper,2)," Q2")
  WHEN left(fiscper,3) IN ("007", "008", "009") THEN CONCAT("FY", RIGHT(fiscper,2)," Q3")
  WHEN left(fiscper,3) IN ("010", "011", "012") THEN CONCAT("FY", RIGHT(fiscper,2)," Q4")
  END AS `FY Q`

  FROM `prd-65343-datalake-bd-88394358.mssfinsapbw_109534_in.109534_mssfin_revenue_metrics_sapbw_in`
  WHERE `zindtry_desc` <> "" AND `industry_group_lgtext` <> "" AND curtype = "20"
  GROUP BY ALL),

afo AS
  (SELECT * 
  FROM rs_sgccr 
  WHERE ABS(`Revenue`) + ABS(`Services Revenue`) + ABS(`Revenue Sharing`) <> 0),

--replicates APPEND notebook

append1 AS  ---Replaces the 'Industry' column from AFO with abbreviations of 'Industry Groups'
(SELECT
`FY Q`,
`Master Client Level 1`,
`Master Client L1 Name`,
`Market`,
`Client Classification`,
`Master Contract`,
`Master Contract Name`,
`Contract`,
`Contract Name`,
`Market Unit`,
`Country_Location`,
`Client Group`,
`Service Group`,
`Industry Group`,
CASE 
WHEN `Industry Group` = "Communications, Media & Technology" THEN "CMT"
WHEN `Industry Group` = "Financial Services" THEN "FS"
WHEN `Industry Group` = "Health & Public Service" THEN "H&PS"
WHEN `Industry Group` = "Products" THEN "PRD"
WHEN `Industry Group` = "Resources" THEN "RES"
END AS Industry,
sum(Revenue) Revenue,
sum(`Services Revenue`) `Services Revenue`,
sum(`Revenue Sharing`) `Revenue Sharing`
from afo
group by ALL),

append2 AS ---Creates ContractID and Sharing Role columns
(Select *,
LEFT(`FY Q`,4) AS `Fiscal Year`,
RIGHT(`FY Q`,2) AS Quarter,
CONCAT(`Contract`,RIGHT(`FY Q`,2),LEFT(`FY Q`,4)) as `Contract ID`,
CASE
WHEN `Revenue Sharing` < 0 THEN "Sender"
WHEN `Revenue Sharing` > 0 THEN "Receiver" 
WHEN `Revenue Sharing` = 0 THEN "Null" END as `Sharing Role`,
CASE WHEN `Revenue Sharing` < 0 THEN ABS(`Revenue Sharing`)
ELSE 0 END AS Sent,
CASE WHEN `Revenue Sharing` > 0 THEN ABS(`Revenue Sharing`)
ELSE 0 END AS Received
FROM append1),

append3 AS --Creates RS activity column that contains the sum per ContractID
(SELECT *,
SUM(Sent + Received) over(partition by `Contract ID`) as `RS Activity`
FROM append2),

append4 AS --Creates Materiality column
(SELECT *,
CASE
WHEN `RS Activity` = 0 THEN "NULL" 
WHEN `RS Activity` > 1000000 THEN "More than 1M"
WHEN `RS Activity` < 100000 THEN "Less than 100K"
WHEN `RS Activity` > 100000 AND `RS Activity` < 1000000 THEN "Less than 1M"
END AS Materiality,
FROM append3),

append5 AS  --Creates the temporary columns which counts the activity of Contract in a country/market/MU/Industry
(SELECT 
        *,
        COUNT(DISTINCT `Country_Location`) OVER (PARTITION BY `Contract ID`) AS country_aux,
        COUNT(DISTINCT Market) OVER (PARTITION BY `Contract ID`) AS market_aux,
        COUNT(DISTINCT `Market Unit`) OVER (PARTITION BY `Contract ID`) AS mu_aux,
        COUNT(DISTINCT Industry) OVER (PARTITION BY `Contract ID`) AS industry_aux,
        SUM(`RS Activity`) OVER (PARTITION BY Contract, `Fiscal Year`) AS activity_aux
    FROM append4
),

append6 as (SELECT 
    *,
    CASE 
        WHEN `RS Activity` = 0 THEN NULL
        WHEN market_aux > 1 THEN 'Cross Market'
        WHEN mu_aux > 1 THEN 'Cross Market Unit'
        WHEN country_aux > 1 THEN 'Cross Country'
        WHEN country_aux = 1 THEN 'Intra Country'
        ELSE NULL
    END AS Sharing_Analysis
    FROM append5),

append7 as (select *,
    CASE 
        WHEN `RS Activity` = 0 THEN NULL
        WHEN Sharing_Analysis = 'Cross Market' THEN 'Cross Market'
        WHEN Sharing_Analysis IS NOT NULL THEN 'Inside Market'
        ELSE NULL
    END AS Simplified_Sharing_Analysis
FROM append6),

append8 as (
select *,
CASE 
        WHEN `RS Activity` = 0 THEN NULL
        WHEN industry_aux > 1 THEN 'Cross Industry'
        WHEN industry_aux = 1 THEN 'Same Industry'
        ELSE NULL
    END AS Industry_Analysis
from append7),

append9 as (select *,
CASE 
WHEN `Fiscal Year` IN ('FY18', 'FY19', 'FY20') AND activity_aux > 0 AND Contract != '#' THEN 1
WHEN `Fiscal Year` NOT IN ('FY18', 'FY19', 'FY20') AND `RS Activity` > 0 AND Contract != '#' THEN 1
ELSE 0 END AS Sharing_Structure
from append8),

append10 as (
select *,
CASE
WHEN CAST(RIGHT(CAST(EXTRACT(YEAR FROM CURRENT_DATE()) AS STRING),2) AS INT64) = CAST(RIGHT(CAST(`Fiscal Year` AS STRING), 2) AS INT64) THEN 1
ELSE 0 end as `Current FY`,
CASE
WHEN CAST(RIGHT(CAST(EXTRACT(YEAR FROM CURRENT_DATE()) AS STRING),2) AS INT64) -1
= CAST(RIGHT(CAST(`Fiscal Year` AS STRING), 2) AS INT64) THEN 1
ELSE 0 end as `Previous FY`,
CASE WHEN CAST(RIGHT(CAST(EXTRACT(YEAR FROM CURRENT_DATE()) AS STRING),2) AS INT64) -2 = CAST(RIGHT(CAST(`Fiscal Year` AS STRING), 2) AS INT64) THEN 1
ELSE 0 end as `Second to Last FY`,
MAX(`FY Q`) OVER() AS current_q_aux
from append9),

append_final as (
SELECT *,
CASE WHEN `Country_Location` = "USA" THEN CONCAT(`Country_Location`," ",`Market Unit`)
ELSE `Country_Location` END AS Location,

CASE WHEN current_q_aux = `FY Q` THEN 1 ELSE 0 END `Current Q`,
CASE WHEN Concat(LEFT(current_q_aux,4)," Q",CAST(RIGHT(current_q_aux,1) AS INT64)-1) = `FY Q` THEN 1 ELSE 0 END `Previous Q`,

---This QTD column gives a value of "1" if this condition is met: record is from previous fiscal year and same quarter
CASE WHEN `Previous FY` = 1 AND Quarter = RIGHT(current_q_aux,2) THEN 1 ELSE 0
end AS QTD,

---This YTD column gives a value of "1" if this condition is met: record is from previous fiscal year and same past quarters
CASE 
WHEN `Previous FY` = 1 AND CAST(RIGHT(QUARTER,1) AS INT64) = CAST(RIGHT(current_q_aux,1) AS INT64) THEN 1
WHEN `Previous FY` = 1 AND CAST(RIGHT(QUARTER,1) AS INT64) < CAST(RIGHT(current_q_aux,1) AS INT64) THEN 1
ELSE 0 END AS YTD
FROM append10),

revshrng as (
SELECT 
`Master Client Level 1`,
`Master Client L1 Name`,
`Market`,
`Client Classification`,
`Master Contract`,
`Master Contract Name`,
`Contract`,
`Contract Name`,
`Market Unit`,
`Country_Location`,
`Client Group`,
`Service Group`,
`Industry Group`,
`Industry`,
`Revenue`,
`Services Revenue`,
`Revenue Sharing`,
`Fiscal Year`,
`Quarter`,
`Current FY`,
`Previous FY`,
`Current Q`,
`Previous Q`,
`Second to Last FY`,
YTD,
QTD,
`Contract ID`,
Location,
`Sharing Role`,
`Sent`,
`Received`,
`RS Activity`,
`Materiality`,
`Sharing_Analysis`,
`Simplified_Sharing_Analysis`,
`Industry_Analysis`,
`Sharing_Structure`
FROM append_final
)

-- Query here

SELECT *
FROM revshrng