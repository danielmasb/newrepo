-- sum by warehouse
SELECT WAREHOUSE_NAME
      ,SUM(CREDITS_USED_COMPUTE) AS CREDITS_USED_COMPUTE_SUM
  FROM snowflake.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY
 WHERE START_TIME >= DATEADD(DAY, -30, CURRENT_TIMESTAMP())  // Past 7 days
 GROUP BY 1
 ORDER BY 2 DESC
;

-- avg by warehouse/hour
SELECT DATE_PART('HOUR', START_TIME) AS START_HOUR
      ,WAREHOUSE_NAME
      ,AVG(CREDITS_USED_COMPUTE) AS CREDITS_USED_COMPUTE_AVG
  FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY
 WHERE START_TIME >= DATEADD(DAY, -30, CURRENT_TIMESTAMP())
   AND WAREHOUSE_ID > 0  // Skip pseudo-VWs such as "CLOUD_SERVICES_ONLY"
 GROUP BY 1, 2
 ORDER BY 3 desc,1
 ;
 
 --sum by hour
 SELECT DATE_PART('HOUR', START_TIME) AS START_HOUR
      ,sum(CREDITS_USED_COMPUTE) AS CREDITS_USED_COMPUTE_AVG
  FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY
 WHERE START_TIME >= DATEADD(DAY, -30, CURRENT_TIMESTAMP())
   AND WAREHOUSE_ID > 0  // Skip pseudo-VWs such as "CLOUD_SERVICES_ONLY"
 GROUP BY 1
 ORDER BY 2 desc,1
 ;
 
 -- num queries by hour
 SELECT DATE_PART('HOUR', START_TIME) AS START_HOUR
      ,COUNT(*) AS NUM_QUERIES
  FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
 WHERE START_TIME >= DATEADD(DAY, -30, CURRENT_TIMESTAMP())
 GROUP BY 1
 ORDER BY 2 desc,1
 ;
 
 ------------------------------------------------------------------------
 -- ######################################################################
 -- ######################################################################
 -- Warehouse Utilization Over 30 Day Average
 
 WITH CTE_DATE_WH AS(
  SELECT TO_DATE(START_TIME) AS START_DATE
        ,WAREHOUSE_NAME
        ,SUM(CREDITS_USED) AS CREDITS_USED_DATE_WH
    FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY
--    WHERE START_TIME >= DATEADD(DAY, -60, CURRENT_TIMESTAMP())  -- 60 days to make the averages
   and DAYNAME(TO_DATE(START_TIME)) NOT IN ('Sat','Sun')      -- to exclude weekends from the average
   GROUP BY START_DATE
           ,WAREHOUSE_NAME
)
SELECT START_DATE
      ,WAREHOUSE_NAME
      ,CREDITS_USED_DATE_WH
      ,AVG(CREDITS_USED_DATE_WH) OVER (PARTITION BY WAREHOUSE_NAME ORDER BY START_DATE ROWS 30 PRECEDING) AS CREDITS_USED_30_DAY_AVG
      ,100.0*((CREDITS_USED_DATE_WH / CREDITS_USED_30_DAY_AVG) - 1) AS PCT_OVER_TO_30_DAY_AVERAGE
  FROM CTE_DATE_WH
WHERE START_DATE >= DATEADD(DAY, -60, CURRENT_TIMESTAMP())
--AND WAREHOUSE_NAME = 'LOOKER_EXTERNAL'
--QUALIFY PCT_OVER_TO_30_DAY_AVERAGE >= 50  // Minimum 50% increase over past 7 day average
 ORDER BY 1,PCT_OVER_TO_30_DAY_AVERAGE DESC
;
 
------------------------------------------------------------------------
-- ######################################################################
-- ######################################################################
-- Approximate credit consumption by client application
WITH CLIENT_HOUR_EXECUTION_CTE AS (
    SELECT  CASE
         WHEN CLIENT_APPLICATION_ID LIKE 'Go %' THEN 'Go'
         WHEN CLIENT_APPLICATION_ID LIKE 'Snowflake UI %' THEN 'Snowflake UI'
         WHEN CLIENT_APPLICATION_ID LIKE 'SnowSQL %' THEN 'SnowSQL'
         WHEN CLIENT_APPLICATION_ID LIKE 'JDBC %' THEN 'JDBC'
         WHEN CLIENT_APPLICATION_ID LIKE 'PythonConnector %' THEN 'Python'
         WHEN CLIENT_APPLICATION_ID LIKE 'ODBC %' THEN 'ODBC'
         ELSE 'NOT YET MAPPED: ' || CLIENT_APPLICATION_ID
       END AS CLIENT_APPLICATION_NAME
    ,WAREHOUSE_NAME
    ,DATE_TRUNC('hour',START_TIME) as START_TIME_HOUR
    ,SUM(EXECUTION_TIME)  as CLIENT_HOUR_EXECUTION_TIME
    FROM "SNOWFLAKE"."ACCOUNT_USAGE"."QUERY_HISTORY" QH
    JOIN "SNOWFLAKE"."ACCOUNT_USAGE"."SESSIONS" SE ON SE.SESSION_ID = QH.SESSION_ID
    WHERE WAREHOUSE_NAME IS NOT NULL
    AND EXECUTION_TIME > 0
  
 --Change the below filter if you want to look at a longer range than the last 1 month 
    AND START_TIME >= DATEADD(DAY, -30, CURRENT_TIMESTAMP())
    group by 1,2,3
    )
, HOUR_EXECUTION_CTE AS (
    SELECT  START_TIME_HOUR
    ,WAREHOUSE_NAME
    ,SUM(CLIENT_HOUR_EXECUTION_TIME) AS HOUR_EXECUTION_TIME
    FROM CLIENT_HOUR_EXECUTION_CTE
    group by 1,2
)
, APPROXIMATE_CREDITS AS (
    SELECT 
    A.CLIENT_APPLICATION_NAME
    ,C.WAREHOUSE_NAME
    ,(A.CLIENT_HOUR_EXECUTION_TIME/B.HOUR_EXECUTION_TIME)*C.CREDITS_USED AS APPROXIMATE_CREDITS_USED

    FROM CLIENT_HOUR_EXECUTION_CTE A
    JOIN HOUR_EXECUTION_CTE B  ON A.START_TIME_HOUR = B.START_TIME_HOUR and B.WAREHOUSE_NAME = A.WAREHOUSE_NAME
    JOIN "SNOWFLAKE"."ACCOUNT_USAGE"."WAREHOUSE_METERING_HISTORY" C ON C.WAREHOUSE_NAME = A.WAREHOUSE_NAME AND C.START_TIME = A.START_TIME_HOUR
)

SELECT 
 CLIENT_APPLICATION_NAME
,WAREHOUSE_NAME
,SUM(APPROXIMATE_CREDITS_USED) AS APPROXIMATE_CREDITS_USED
FROM APPROXIMATE_CREDITS
GROUP BY 1,2
ORDER BY 3 DESC
;
 
 
 
 ------------------------------------------------------------------------
 -- APPROXIMATE CREDIT CONSUMPTION BY USER
WITH USER_HOUR_EXECUTION_CTE AS (
    SELECT  USER_NAME
    ,WAREHOUSE_NAME
    ,DATE_TRUNC('hour',START_TIME) as START_TIME_HOUR
    ,SUM(EXECUTION_TIME)  as USER_HOUR_EXECUTION_TIME
    FROM "SNOWFLAKE"."ACCOUNT_USAGE"."QUERY_HISTORY" 
    WHERE WAREHOUSE_NAME IS NOT NULL
    AND EXECUTION_TIME > 0
  
 --Change the below filter if you want to look at a longer range than the last 1 month 
    AND START_TIME >= DATEADD(DAY, -30, CURRENT_TIMESTAMP()) 
    group by 1,2,3
    )
, HOUR_EXECUTION_CTE AS (
    SELECT  START_TIME_HOUR
    ,WAREHOUSE_NAME
    ,SUM(USER_HOUR_EXECUTION_TIME) AS HOUR_EXECUTION_TIME
    FROM USER_HOUR_EXECUTION_CTE
    group by 1,2
)
, APPROXIMATE_CREDITS AS (
    SELECT 
    A.USER_NAME
    ,C.WAREHOUSE_NAME
    ,(A.USER_HOUR_EXECUTION_TIME/B.HOUR_EXECUTION_TIME)*C.CREDITS_USED AS APPROXIMATE_CREDITS_USED

    FROM USER_HOUR_EXECUTION_CTE A
    JOIN HOUR_EXECUTION_CTE B  ON A.START_TIME_HOUR = B.START_TIME_HOUR and B.WAREHOUSE_NAME = A.WAREHOUSE_NAME
    JOIN "SNOWFLAKE"."ACCOUNT_USAGE"."WAREHOUSE_METERING_HISTORY" C ON C.WAREHOUSE_NAME = A.WAREHOUSE_NAME AND C.START_TIME = A.START_TIME_HOUR
)

SELECT 
 USER_NAME
,WAREHOUSE_NAME
,SUM(APPROXIMATE_CREDITS_USED) AS APPROXIMATE_CREDITS_USED
FROM APPROXIMATE_CREDITS
GROUP BY 1,2
ORDER BY 3 DESC
;

 ------------------------------------------------------------------------
-- Queries by # of Times Executed and Execution Time

SELECT 
QUERY_TEXT
,count(*) as number_of_queries
,sum(TOTAL_ELAPSED_TIME)/1000 as execution_seconds
,sum(TOTAL_ELAPSED_TIME)/(1000*60) as execution_minutes
,sum(TOTAL_ELAPSED_TIME)/(1000*60*60) as execution_hours

  from SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY Q
  where 1=1
  and TO_DATE(Q.START_TIME) > DATEADD(DAY, -30, CURRENT_TIMESTAMP())  
 and TOTAL_ELAPSED_TIME > 0 --only get queries that actually used compute
  group by 1
  having count(*) >= 10 --configurable/minimal threshold
  order by 2 desc
  limit 100 --configurable upper bound threshold
  ;

------------------------------------------------------------------------
-- Top Longest Running Queries
select
          
 QUERY_ID
,'https://'||CURRENT_ACCOUNT()||'.snowflakecomputing.com/console#/monitoring/queries/detail?queryId='||Q.QUERY_ID as QUERY_PROFILE_URL
,ROW_NUMBER() OVER(ORDER BY PARTITIONS_SCANNED DESC) as QUERY_ID_INT
,QUERY_TEXT
,TOTAL_ELAPSED_TIME/1000 AS QUERY_EXECUTION_TIME_SECONDS
,PARTITIONS_SCANNED
,PARTITIONS_TOTAL

from SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY Q
 where 1=1
  and TO_DATE(Q.START_TIME) > DATEADD(DAY, -30, CURRENT_TIMESTAMP()) 
    and TOTAL_ELAPSED_TIME > 0 --only get queries that actually used compute
    and ERROR_CODE iS NULL
    and PARTITIONS_SCANNED is not null
   
  order by  TOTAL_ELAPSED_TIME desc
   
LIMIT 50
;

------------------------------------------------------------------------
-- ######################################################################
-- ######################################################################
-- Queries by Execution Buckets over the Past 30 Days
WITH BUCKETS AS (

SELECT 'Less than 1 second' as execution_time_bucket, 0 as execution_time_lower_bound, 1000 as execution_time_upper_bound
UNION ALL
SELECT '1-5 seconds' as execution_time_bucket, 1000 as execution_time_lower_bound, 5000 as execution_time_upper_bound
UNION ALL
SELECT '5-10 seconds' as execution_time_bucket, 5000 as execution_time_lower_bound, 10000 as execution_time_upper_bound
UNION ALL
SELECT '10-20 seconds' as execution_time_bucket, 10000 as execution_time_lower_bound, 20000 as execution_time_upper_bound
UNION ALL
SELECT '20-30 seconds' as execution_time_bucket, 20000 as execution_time_lower_bound, 30000 as execution_time_upper_bound
UNION ALL
SELECT '30-60 seconds' as execution_time_bucket, 30000 as execution_time_lower_bound, 60000 as execution_time_upper_bound
UNION ALL
SELECT '1-2 minutes' as execution_time_bucket, 60000 as execution_time_lower_bound, 120000 as execution_time_upper_bound
UNION ALL
SELECT 'more than 2 minutes' as execution_time_bucket, 120000 as execution_time_lower_bound, NULL as execution_time_upper_bound
)

SELECT 
 COALESCE(execution_time_bucket,'more than 2 minutes')
,count(Query_ID) as number_of_queries

from SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY Q
FULL OUTER JOIN BUCKETS B ON (Q.TOTAL_ELAPSED_TIME) >= B.execution_time_lower_bound and (Q.TOTAL_ELAPSED_TIME) < B.execution_time_upper_bound
where Q.Query_ID is null
OR (
TO_DATE(Q.START_TIME) >= DATEADD(DAY, -30, CURRENT_TIMESTAMP())
and warehouse_name = 'LOOKER_EXTERNAL'
and TOTAL_ELAPSED_TIME > 0 
  )
group by 1,COALESCE(b.execution_time_lower_bound,120000)
order by COALESCE(b.execution_time_lower_bound,120000)
;


------------------------------------------------------------------------
-- Warehouses with High Cloud Services Usage
    -- Focus on Warehouses that are using a high volume and ratio of cloud services compute. 
    -- Investigate why this is the case to reduce overall cost (might be cloning, listing files in S3, partner tools setting session parameters, etc.). 
    -- The goal to reduce cloud services credit consumption is to aim for cloud services credit to be less than 10% of overall credits.

select 
    WAREHOUSE_NAME
    ,SUM(CREDITS_USED) as CREDITS_USED
    ,SUM(CREDITS_USED_CLOUD_SERVICES) as CREDITS_USED_CLOUD_SERVICES
    ,SUM(CREDITS_USED_CLOUD_SERVICES)/SUM(CREDITS_USED) as PERCENT_CLOUD_SERVICES
from "SNOWFLAKE"."ACCOUNT_USAGE"."WAREHOUSE_METERING_HISTORY"
where TO_DATE(START_TIME) >= DATEADD(DAY, -30, CURRENT_TIMESTAMP()) 
and CREDITS_USED_CLOUD_SERVICES > 0
group by 1
order by 4 desc
;

------------------------------------------------------------------------
------------------------------------------------------------------------
------------------------------------------------------------------------
-- Warehouses queries
use warehouse LOOKER;

select *
from table(information_schema.warehouse_load_history(date_range_start=>dateadd('hour',-1,current_timestamp())));

------------------------------------------------------------------------
-- Warehouses queries by hour
SELECT 
       DATE_TRUNC('hour', start_time) start_time_trunced_at_hour,
       HOUR(start_time)               start_time_hour,
       warehouse_name,
       sum(avg_running)               sum_running_queries,
       sum(avg_queued_load)           sum_queued_load_queries,
       sum(avg_queued_provisioning)   sum_queued_provisioning_queries,
       sum(avg_blocked)               sum_blocked_queries
 --      avg(avg_running)               avg_running_queries,
 --      avg(avg_queued_load)           avg_queued_load_queries,
 --      AVG(avg_queued_provisioning)   avg_queued_provisioning_queries,
 --      AVG(avg_blocked)               avg_blocked_queries
 FROM snowflake.account_usage.warehouse_load_history
WHERE DATE_TRUNC('DAY', start_time) >= '2021-03-01'
  --AND warehouse_name = 'LOOKER_EXTERNAL'
GROUP BY warehouse_name, start_time_trunced_at_hour, start_time_hour
ORDER BY 1,3;

------------------------------------------------------------------------
-- 
