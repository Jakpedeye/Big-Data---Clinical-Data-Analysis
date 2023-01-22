-- Databricks notebook source
CREATE EXTERNAL TABLE clinicaltrial_2021
    (id STRING,
    sponsor STRING,
    status STRING, 
    START STRING,
    COMPLETION STRING,
    TYPE STRING,
    submission STRING,
    conditions STRING,
    interventions STRING)
    using csv
  options (header="True",
  delimiter = "|",
  inferschema = "True",
  path = "/FileStore/tables/clinicaltrial_2021.csv");

-- COMMAND ----------

SELECT * FROM clinicaltrial_2021

-- COMMAND ----------

CREATE TABLE pharma 
    (Company STRING,
    Parent_Company STRING,
    Penalty_Amount STRING,
    Subtraction_From_Penalty STRING,
    Penalty_Amount_Adjusted_For_Eliminating_Multiple_Counting STRING,
    Penalty_Year INT, 
    Penalty_Date DATE, 
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
    Notes STRING)
    using csv
options (header = "true",
delimeter = ",", 
path = "/FileStore/tables/pharma.csv");

-- COMMAND ----------

SELECT * FROM pharma

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS mesh(term string, tree string)
using csv
options (header="true",
delimiter=",",
path="/FileStore/tables/mesh.csv");

-- COMMAND ----------

SELECT * FROM mesh

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## TASK 1
-- MAGIC Number of Studies

-- COMMAND ----------

SELECT COUNT(DISTINCT Id) Number_of_Studies
FROM clinicaltrial_2021
WHERE Id != 'Id' 


-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## TASK 2
-- MAGIC Types of Studies sand their frequency

-- COMMAND ----------

SELECT type,
         COUNT(type) Frequency
  FROM clinicaltrial_2021
GROUP BY type
HAVING TYPE != 'type'
ORDER BY Frequency DESC;


-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## TASK 3
-- MAGIC Top 5 Conditions

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW Splitted_Conditions AS SELECT explode(split(conditions, ',')) as split_conditions
FROM clinicaltrial_2021

-- COMMAND ----------

select * from splitted_conditions

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Top 5 Conditions

-- COMMAND ----------

select split_conditions, count(split_conditions) from splitted_conditions
group by split_conditions
order by count(split_conditions) desc
limit 5

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## TASK 4

-- COMMAND ----------

select left(tree, 3) as root, count(*) as frequency
from mesh
inner join splitted_conditions on Splitted_conditions.split_conditions = mesh.term
group by root
order by frequency desc
limit 5

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##Task 5
-- MAGIC 10 most common sponsors that are not pharmaceutical companies along with the number
-- MAGIC of clinical trials they have sponsored

-- COMMAND ----------

SELECT Sponsor,
       COUNT(Sponsor) frequency
FROM clinicaltrial_2021
WHERE Sponsor NOT IN
    (SELECT DISTINCT Parent_Company
     FROM pharma)
GROUP BY 1
ORDER BY 2 DESC
LIMIT 10;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##Task 6
-- MAGIC Plot of Completed studies

-- COMMAND ----------

WITH MonthData AS (
  SELECT
    REGEXP_extract(Completion, '[A-Za-z]+', 0) Months
  from
    clinicaltrial_2021
  WHERE
    COMPLETION LIKE '%2021'
    AND status = 'Completed'),
MonthsOrder AS (
  SELECT
    Months,
    COUNT(*) `Studies Completed`,
    CASE
      WHEN Months = 'Jan' THEN 1
      WHEN Months = 'Feb' THEN 2
      WHEN Months = 'Mar' THEN 3
      WHEN Months = 'Apr' THEN 4
      WHEN Months = 'May' THEN 5
      WHEN Months = 'Jun' THEN 6
      WHEN Months = 'Jul' THEN 7
      WHEN Months = 'Aug' THEN 8
      WHEN Months = 'Sep' THEN 9
      WHEN Months = 'Oct' THEN 10
      WHEN Months = 'Nov' THEN 11
      WHEN Months = 'Dec' THEN 12
    END OrderOfMonths
  FROM
    MonthData
  GROUP BY
    Months
  ORDER BY
    OrderOfMonths)
SELECT
  Months,
  `Studies Completed`
FROM
  MonthsOrder;

-- COMMAND ----------


