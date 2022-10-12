-- Databricks notebook source
SET fileroot = clinicaltrial_2021

-- COMMAND ----------

DROP TABLE if  exists clinicaltrial_2021

-- COMMAND ----------


CREATE External TABLE IF NOT EXISTS clinicaltrial_2021(
Id STRING ,
Sponsor STRING ,
Status STRING ,
Start STRING ,
Completion STRING ,
Type STRING ,
Submission STRING ,
Conditions STRING ,
Interventions STRING ) USING CSV
OPTIONS (path "/FileStore/tables/${hiveconf:fileroot}.csv",
        delimiter "|",
        header "true")
        ;

-- COMMAND ----------

-- DBTITLE 1,Q1 STUDIES

select count(*)
from clinicaltrial_2021

-- COMMAND ----------


select * from clinicaltrial_2021

-- COMMAND ----------

-- DBTITLE 1,Q2 TYPES

Select type, COUNT(*) FROM clinicaltrial_2021
Group BY type ORDER BY COUNT(*) DESC;

-- COMMAND ----------

-- DBTITLE 1,Q3 Conditions

SELECT DISEASES, COUNT(DISEASES) as Frequency
FROM clinicaltrial_2021
LATERAL VIEW explode(split(Conditions,',')) Conditions as DISEASES
GROUP BY DISEASES
ORDER BY Frequency DESC Limit 5;

-- COMMAND ----------

-- DBTITLE 1,Q4 Roots
CREATE External TABLE IF NOT EXISTS  NEWMESH(
TERM STRING,
TREE STRING)
USING CSV 
OPTIONS (path "dbfs:/FileStore/tables/mesh.csv",
        delimiter ",",
        header "true")
        ;
desc NEWMESH

-- COMMAND ----------

Select *
from NEWMESH 
LIMIT 5;

-- COMMAND ----------

CREATE OR REPLACE TABLE sub_string2012(
SELECT TERM,TREE,SUBSTRING(TREE,1,3) AS MAIN1
FROM NEWMESH);
select * from sub_String2012

-- COMMAND ----------

CREATE OR REPLACE TABLE EXPLODE_CONDITIONNEW(SELECT Id,Condition from clinicaltrial_2021
LATERAL VIEW explode(split(Conditions,',')) Conditions as Condition);
Select * from EXPLODE_CONDITIONNEW

-- COMMAND ----------

SELECT MAIN1,COUNT(MAIN1) AS TOTAL 
From EXPLODE_CONDITIONNEW
INNER JOIN sub_String2012 ON EXPLODE_CONDITIONNEW.Condition = sub_String2012.TERM
GROUP BY MAIN1
ORDER BY TOTAL DESC
LIMIT 5

-- COMMAND ----------

-- DBTITLE 1,Creating Phamratable Q5 NON PHARMA
CREATE External TABLE IF NOT EXISTS pharma (
Company STRING,
Parent_Company STRING, 
Penalty_Amount STRING, 
Subtraction_From_Penalty STRING, 
Penalty_Amount_Adjusted_For_Eliminating_Multiple_Counting STRING, 
Penalty_Year STRING, 
Penalty_Date STRING, 
Offense_Group STRING, 
Primary_Offense STRING, 
Secondary_Offense STRING, 
Description STRING,
Level_of_Government STRING, 
Action_Type STRING, 
Agency STRING, 
Civil_Criminal STRING, 
Prosecution_Agreement STRING,
Court STRING,
Case_ID STRING,
Private_Litigation_Case_Title STRING,
Lawsuit_Resolution STRING,
Facility_State STRING,
City STRING,
Address STRING,
Zip STRING,
NAICS_Code STRING,
NAICS_Translation STRING,
HQ_Country_of_Parent STRING,
HQ_State_of_Parent STRING,
Ownership_Structure STRING,
Parent_Company_Stock_Ticker STRING,
Major_Industry_of_Parent STRING,
Specific_Industry_of_Parent STRING,
Info_Source STRING,
Notes STRING
) USING CSV
OPTIONS (path "dbfs:/FileStore/tables/pharma.csv",
        delimiter ",",
        header "true")
        ;
SELECT * FROM pharma limit 5;

-- COMMAND ----------

CREATE OR REPLACE TABLE
Pharma_main33(SELECT Parent_Company from pharma);
SELECT * FROM Pharma_main33

-- COMMAND ----------

CREATE OR REPLACE TABLE SPONSERS22(Select Sponsor FROM clinicaltrial_2021
);
SELECT * FROM SPONSERS22

-- COMMAND ----------

SELECT Sponsor,COUNT(Sponsor)AS Total 
From SPONSERS22
Left Join Pharma_main33 ON SPONSERS22.Sponsor = Pharma_main33.Parent_Company
WHERE Parent_Company IS NULL 
Group By Sponsor 
ORDER BY TOTAL DESC
LIMIT 10

-- COMMAND ----------

-- DBTITLE 1,Q6 COMPLETED STUDIES STATUS
CREATE OR REPLACE TABLE STATUS_COMPLETE1212
(SELECT split(Completion,' ')[0] as Month ,split(Completion,' ')[1] as YEAR,STATUS
FROM clinicaltrial_2021);
Select * from STATUS_COMPLETE1212

-- COMMAND ----------

SELECT Month,COUNT(MONTH)AS YEAR
FROM STATUS_COMPLETE1212
WHERE YEAR == 2021  AND STATUS == 'Completed'
GROUP BY Month
Order by unix_timestamp(Month,'MMM') asc

-- COMMAND ----------

SELECT Month,COUNT(MONTH)AS COUNT
FROM STATUS_COMPLETE1212
WHERE YEAR == 2021 AND STATUS == 'Completed'
GROUP BY Month
Order by unix_timestamp(Month,'MMM') asc
