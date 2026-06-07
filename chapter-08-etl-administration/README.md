# Chapter 8: ETL Administration — Scheduling, Hierarchies, Aggregates, and Migrations

> **ETL for Business Intelligence**
> *A practical guide to data provisioning, dimensional modelling, and pipeline design*
>
> © Patrick Dolinger, NSCC Institute of Technology
> Licensed under [Creative Commons Attribution 4.0 International (CC BY 4.0)](https://creativecommons.org/licenses/by/4.0/)
> You are free to share and adapt this material for any purpose, provided appropriate credit is given.

---

## Chapter Overview

The previous chapters built a complete, documented, and tested ETL pipeline with advanced transformation capabilities. This chapter addresses the operational side of ETL: how a pipeline is scheduled and monitored in production, how hierarchies and aggregate tables extend the analytical value of the warehouse, and how ETL systems are promoted safely through development, test, and production environments.

These are the disciplines that separate a working ETL system from a *production ETL system* — one that runs reliably, night after night, with monitoring, alerting, and governance supporting it.

By the end of this chapter you will be able to:

- Configure and manage SQL Server Agent jobs to schedule SSIS packages
- Monitor ETL execution using the SSIS Catalog and Agent job history
- Design and implement aggregate tables for analytical performance
- Describe balanced and ragged hierarchies and implement them in a dimensional model
- Explain the DEV → Test → QA → Production promotion process
- Use SSIS environment variables to manage connection strings across environments
- Implement a rollback strategy for failed ETL deployments
- Describe best practices for ETL performance tuning

---

## Table of Contents

1. [ETL in Production: The Operational Mindset](#1-etl-in-production-the-operational-mindset)
2. [SQL Server Agent: Scheduling ETL](#2-sql-server-agent-scheduling-etl)
3. [Monitoring and Alerting](#3-monitoring-and-alerting)
4. [Hierarchies in Dimensional Models](#4-hierarchies-in-dimensional-models)
5. [Aggregate Tables and Performance](#5-aggregate-tables-and-performance)
6. [The SSIS Catalog: Deployment and Configuration](#6-the-ssis-catalog-deployment-and-configuration)
7. [Environment Promotion: DEV → Test → QA → Production](#7-environment-promotion-dev--test--qa--production)
8. [ETL Performance Tuning](#8-etl-performance-tuning)
9. [ETL Versioning and Rollback](#9-etl-versioning-and-rollback)
10. [Chapter Summary](#10-chapter-summary)
11. [Review Questions](#11-review-questions)
12. [🔍 Deeper Dive](#-deeper-dive)

---

## 1. ETL in Production: The Operational Mindset

An ETL system in production is not a project — it is a service. A service has users who depend on it, SLAs (service level agreements) that define acceptable performance and availability, and operational procedures that govern how it is managed, changed, and recovered.

This shift from project mindset to service mindset changes how ETL developers think about their work:

| Project mindset | Service mindset |
|---|---|
| "It runs without errors" | "It runs reliably, within the SLA, every night" |
| "I know how to fix it if it breaks" | "The on-call engineer can diagnose and fix it without my involvement" |
| "I can change it quickly when needed" | "Changes are tested, documented, and approved before deployment" |
| "The data looks right" | "The data is verified by automated tests after every load" |
| "I'll document it later" | "Documentation is current and reviewed regularly" |

Every technique in this chapter — scheduling, monitoring, environment promotion, rollback — exists in service of this operational mindset.

### 1.1 The Production ETL Contract

A production ETL system carries an implicit contract with its users:

- **Freshness:** Data will be available in the DW within a defined window after source data changes (e.g., "DW reflects previous day's transactions by 6:00 AM")
- **Completeness:** All source records within scope will be loaded — no silent drops
- **Accuracy:** All transformations and derivations are correct per the agreed S2T mapping
- **Availability:** The DW is queryable throughout the business day (ETL runs during off-peak hours)
- **Notification:** Failures are reported immediately — business users are not the first to discover a problem

Each element of the contract drives a specific operational practice. Freshness drives the scheduling strategy. Completeness drives the reconciliation test suite. Accuracy drives the S2T mapping and derived measure verification. Availability drives the batch window management. Notification drives the alerting configuration.

---

## 2. SQL Server Agent: Scheduling ETL

**SQL Server Agent** is the built-in job scheduler for SQL Server. It runs as a Windows service and executes jobs on defined schedules — nightly, hourly, or on demand. For ETL, it is the mechanism that runs the master SSIS package automatically without human intervention.

### 2.1 Job Architecture

A SQL Server Agent job consists of:

- **Job:** The top-level unit — named, described, and owned by a login
- **Steps:** One or more ordered steps within the job. Each step executes a specific action (run an SSIS package, execute T-SQL, run a PowerShell script)
- **Schedule:** One or more schedules that define when the job runs automatically
- **Notifications:** Email or event log entries triggered on job success, failure, or completion
- **History:** A log of every job execution — start time, end time, duration, success/failure, and step-level messages

### 2.2 Creating the Nightly ETL Job

The following steps create a SQL Server Agent job that executes the CabotTrail master ETL package nightly at 2:00 AM.

**Step 1 — Create the job:**

```sql
USE msdb;
GO

EXEC sp_add_job
    @job_name           = N'CabotTrail_DW_Nightly_Load',
    @description        = N'Loads CabotTrailOutdoorDW from CabotTrailOutdoor OLTP.
                            Full reload of all dimensions and facts.
                            Runs nightly at 02:00. Alerts on failure.',
    @owner_login_name   = N'sa',   -- or a dedicated ETL service account
    @notify_level_email = 2,       -- 2 = On failure
    @notify_email_operator_name = N'ETL Operations';
GO
```

**Step 2 — Add the SSIS package execution step:**

```sql
EXEC sp_add_jobstep
    @job_name       = N'CabotTrail_DW_Nightly_Load',
    @step_name      = N'Execute Master ETL Package',
    @step_id        = 1,
    @subsystem      = N'SSIS',
    @command        = N'/ISSERVER "\"\SSISDB\CabotTrailETL\Packages\Master_DW_Load.dtsx\""
                       /SERVER localhost
                       /ENVREFERENCE 1',    -- Environment reference ID
    @on_success_action = 1,  -- 1 = Quit reporting success
    @on_fail_action    = 2;  -- 2 = Quit reporting failure
GO
```

**Step 3 — Add the schedule:**

```sql
EXEC sp_add_schedule
    @schedule_name      = N'Daily_2AM',
    @freq_type          = 4,        -- 4 = Daily
    @freq_interval      = 1,        -- Every 1 day
    @active_start_time  = 020000,   -- 02:00:00
    @active_end_time    = 235959;   -- Run until end of day if needed
GO

EXEC sp_attach_schedule
    @job_name       = N'CabotTrail_DW_Nightly_Load',
    @schedule_name  = N'Daily_2AM';
GO

EXEC sp_add_jobserver
    @job_name       = N'CabotTrail_DW_Nightly_Load',
    @server_name    = N'(local)';
GO
```

**Step 4 — Set up the email operator:**

```sql
-- Create the ETL Operations notification operator
EXEC sp_add_operator
    @name                   = N'ETL Operations',
    @enabled                = 1,
    @email_address          = N'Patrick.Dolinger@nscc.ca',
    @pager_days             = 62;   -- Mon-Fri
GO
```

### 2.3 The ETL Job Schedule Design

The schedule must balance two competing concerns:

**The load window must be long enough** to complete the ETL plus all reconciliation tests, with capacity for at least one retry on failure.

**The load must complete before business users arrive.** In most organizations, the DW must be current by 6:00–8:00 AM. If the load starts at 2:00 AM and typically completes in 45 minutes, there is a 3–4 hour window for retries and recovery.

For CabotTrail, the full load completes in under 5 minutes (16,359 fact rows is modest). A 2:00 AM start provides ample recovery time before a 6:00 AM business start.

For large-scale systems, the schedule design becomes more complex:

```sql
-- Multi-step job: dimensions load first, then facts
-- Allows dimension failures to be caught before fact loads begin

EXEC sp_add_jobstep
    @job_name           = N'CabotTrail_DW_Nightly_Load',
    @step_name          = N'Step 1: Load Dimensions',
    @step_id            = 1,
    @subsystem          = N'SSIS',
    @command            = N'... Master_DimLoad.dtsx ...',
    @on_success_action  = 3,    -- 3 = Go to next step
    @on_fail_action     = 2;    -- 2 = Quit reporting failure

EXEC sp_add_jobstep
    @job_name           = N'CabotTrail_DW_Nightly_Load',
    @step_name          = N'Step 2: Load Facts',
    @step_id            = 2,
    @subsystem          = N'SSIS',
    @command            = N'... Master_FactLoad.dtsx ...',
    @on_success_action  = 1,    -- 1 = Quit reporting success
    @on_fail_action     = 2;    -- 2 = Quit reporting failure
GO
```

### 2.4 On-Demand Execution

In addition to scheduled runs, ETL must sometimes be executed on demand — after a data correction, a failed overnight run, or a one-time historical reload. SQL Server Agent supports manual execution:

```sql
-- Execute the job immediately (on demand)
EXEC msdb.dbo.sp_start_job
    @job_name = N'CabotTrail_DW_Nightly_Load';
```

For cases where only a specific package needs reloading (e.g., only `DimCustomer` needs refreshing), a stored procedure wrapper provides controlled on-demand execution:

```sql
-- On-demand dimension refresh procedure
CREATE PROCEDURE dbo.usp_RefreshDimension
    @DimensionName      NVARCHAR(100),
    @EnvironmentRef     INT = 1         -- Default to Production environment
AS
BEGIN
    DECLARE @ExecutionID BIGINT;

    -- Create an execution in the SSIS Catalog
    EXEC SSISDB.catalog.create_execution
        @package_name       = @DimensionName,
        @execution_id       = @ExecutionID OUTPUT,
        @folder_name        = N'CabotTrailETL',
        @project_name       = N'CabotTrailETL',
        @use32bitruntime    = FALSE,
        @reference_id       = @EnvironmentRef;

    -- Start the execution
    EXEC SSISDB.catalog.start_execution
        @execution_id = @ExecutionID;

    -- Return the execution ID for monitoring
    SELECT @ExecutionID AS ExecutionID;
END;
GO

-- Usage
EXEC dbo.usp_RefreshDimension @DimensionName = 'Load_DimCustomer.dtsx';
```

---

## 3. Monitoring and Alerting

A production ETL system must be actively monitored. "No news is good news" is not an acceptable monitoring strategy — a failed load that is not reported is a load that business users will discover when their dashboards show yesterday's data.

### 3.1 Three Layers of Monitoring

**Layer 1: Job-level monitoring (SQL Server Agent)**
Did the job start? Did it finish? Did it report success or failure?

**Layer 2: Package-level monitoring (SSIS Catalog)**
Which packages ran? How long did each take? Did any package log errors?

**Layer 3: Business-level monitoring (ETL.TestResults)**
Did the data load correctly? Do row counts match? Does revenue reconcile?

All three layers are necessary. A job that reports success is not necessarily a job that loaded correct data — a package can succeed while silently dropping rows into error tables.

### 3.2 Job History Queries

```sql
-- Job execution history: recent runs with status and duration
USE msdb;

SELECT
    j.name                                  AS JobName,
    jh.run_date,
    -- Format run_time as HH:MM:SS
    STUFF(STUFF(RIGHT('000000' + CAST(jh.run_time AS VARCHAR), 6), 5, 0, ':'), 3, 0, ':')
                                            AS RunTime,
    -- Format run_duration as HH:MM:SS
    STUFF(STUFF(RIGHT('000000' + CAST(jh.run_duration AS VARCHAR), 6), 5, 0, ':'), 3, 0, ':')
                                            AS Duration,
    CASE jh.run_status
        WHEN 0 THEN '❌ Failed'
        WHEN 1 THEN '✅ Succeeded'
        WHEN 2 THEN '🔄 Retry'
        WHEN 3 THEN '⚠️  Cancelled'
        WHEN 4 THEN '⏳ Running'
    END                                     AS Status,
    jh.message
FROM    sysjobs j
INNER JOIN sysjobhistory jh
    ON jh.job_id = j.job_id
WHERE   j.name = N'CabotTrail_DW_Nightly_Load'
AND     jh.step_id = 0      -- 0 = job-level outcome (not individual steps)
ORDER BY jh.run_date DESC, jh.run_time DESC;
```

```sql
-- Detect jobs that did not run when expected
-- (Identifies missed schedules — a job that should have run at 2 AM but did not)
SELECT
    j.name                      AS JobName,
    CAST(s.next_run_date AS VARCHAR) + ' ' +
        STUFF(STUFF(RIGHT('000000' + CAST(s.next_run_time AS VARCHAR), 6), 5, 0, ':'), 3, 0, ':')
                                AS NextScheduledRun,
    ISNULL(
        CAST(last_run.run_date AS VARCHAR) + ' ' +
        STUFF(STUFF(RIGHT('000000' + CAST(last_run.run_time AS VARCHAR), 6), 5, 0, ':'), 3, 0, ':'),
        'Never'
    )                           AS LastActualRun,
    CASE last_run.run_status WHEN 1 THEN 'Success' ELSE 'Failed/Unknown' END AS LastStatus
FROM    sysjobs j
INNER JOIN sysjobschedules js   ON js.job_id = j.job_id
INNER JOIN sysschedules s       ON s.schedule_id = js.schedule_id
LEFT JOIN (
    SELECT job_id, run_date, run_time, run_status,
           ROW_NUMBER() OVER (PARTITION BY job_id ORDER BY run_date DESC, run_time DESC) AS rn
    FROM sysjobhistory WHERE step_id = 0
) last_run ON last_run.job_id = j.job_id AND last_run.rn = 1
WHERE   j.name LIKE N'CabotTrail%';
```

### 3.3 SSIS Catalog Monitoring Queries

```sql
-- Package execution summary: last 7 days
USE SSISDB;

SELECT
    e.package_name,
    e.start_time,
    e.end_time,
    DATEDIFF(SECOND, e.start_time, e.end_time)  AS DurationSeconds,
    CASE e.status
        WHEN 7 THEN '✅ Succeeded'
        WHEN 4 THEN '❌ Failed'
        WHEN 2 THEN '⏳ Running'
        WHEN 3 THEN '⚠️  Cancelled'
        ELSE        '⚙️  Other (' + CAST(e.status AS VARCHAR) + ')'
    END                                         AS StatusLabel
FROM    catalog.executions e
WHERE   e.start_time >= DATEADD(DAY, -7, GETDATE())
ORDER BY e.start_time DESC;
```

```sql
-- Error messages from the most recent failed execution
SELECT  TOP 1 execution_id INTO #LastFailed
FROM    SSISDB.catalog.executions
WHERE   status = 4      -- Failed
ORDER BY start_time DESC;

SELECT
    om.message_time,
    om.package_name,
    om.task_name,
    om.message
FROM    SSISDB.catalog.operation_messages om
WHERE   om.operation_id = (SELECT execution_id FROM #LastFailed)
AND     om.message_type IN (120, 130)   -- 120=Error, 130=Warning
ORDER BY om.message_time;

DROP TABLE #LastFailed;
```

```sql
-- Performance trend: daily average load duration
SELECT
    CAST(start_time AS DATE)                        AS LoadDate,
    COUNT(*)                                        AS ExecutionCount,
    AVG(DATEDIFF(SECOND, start_time, end_time))     AS AvgDurationSeconds,
    MAX(DATEDIFF(SECOND, start_time, end_time))     AS MaxDurationSeconds,
    SUM(CASE WHEN status = 7 THEN 1 ELSE 0 END)     AS Successes,
    SUM(CASE WHEN status = 4 THEN 1 ELSE 0 END)     AS Failures
FROM    SSISDB.catalog.executions
WHERE   package_name = N'Master_DW_Load.dtsx'
AND     start_time >= DATEADD(DAY, -30, GETDATE())
GROUP BY CAST(start_time AS DATE)
ORDER BY LoadDate DESC;
```

### 3.4 The Operational Dashboard Query

A single query that gives the on-call engineer an immediate picture of last night's load:

```sql
-- ETL Operational Dashboard: last load status
USE CabotTrailOutdoorDW;

SELECT
    '── LAST LOAD RUN ──────────────────────────────' AS Section,
    NULL AS Detail
UNION ALL
SELECT 'Run Status',
    RunStatus + ' | Tests: ' +
    CAST(TestsPassed AS VARCHAR) + ' passed, ' +
    CAST(TestsFailed AS VARCHAR) + ' failed | ' +
    'Duration: ' +
    CAST(DATEDIFF(SECOND, RunStartTime, RunEndTime) AS VARCHAR) + 's'
FROM ETL.LoadRuns
WHERE RunID = (SELECT MAX(RunID) FROM ETL.LoadRuns)
UNION ALL
SELECT '── FAILED TESTS (if any) ─────────────────────', NULL
UNION ALL
SELECT TestName + ' [' + TargetTable + ']',
    'Expected: ' + ExpectedValue + ' | Actual: ' + ActualValue
FROM ETL.TestResults
WHERE Passed = 0
AND   CAST(RunDate AS DATE) = CAST((SELECT MAX(RunStartTime) FROM ETL.LoadRuns) AS DATE)
UNION ALL
SELECT '── TABLE FRESHNESS ───────────────────────────', NULL
UNION ALL
SELECT s.name + '.' + t.name,
    CAST(p.rows AS VARCHAR) + ' rows | Last loaded: ' +
    ISNULL(CAST(MAX(tr.RunDate) AS VARCHAR), 'Unknown')
FROM sys.tables t
INNER JOIN sys.schemas s ON s.schema_id = t.schema_id
INNER JOIN sys.partitions p ON p.object_id = t.object_id AND p.index_id IN (0,1)
LEFT  JOIN ETL.TestResults tr
    ON tr.TargetTable = s.name + '.' + t.name
    AND tr.TestCategory = 'RowCount' AND tr.Passed = 1
WHERE s.name IN ('Dimension','Fact')
GROUP BY s.name, t.name, p.rows
ORDER BY Section, Detail;
```

### 3.5 Database Mail for Alerting

Database Mail must be configured before SQL Server Agent notifications work. Once configured, it sends emails automatically on job failure:

```sql
-- Configure Database Mail (requires sysadmin permissions)
EXEC msdb.dbo.sysmail_add_account_sp
    @account_name           = N'ETL Alert Account',
    @description            = N'Account for ETL failure notifications',
    @email_address          = N'etl-alerts@nscc.ca',
    @display_name           = N'CabotTrail ETL Alerts',
    @mailserver_name        = N'smtp.nscc.ca',
    @port                   = 587,
    @enable_ssl             = 1,
    @username               = N'etl-service@nscc.ca',
    @password               = N'[service_account_password]';

EXEC msdb.dbo.sysmail_add_profile_sp
    @profile_name = N'ETL Alerts',
    @description  = N'Profile for ETL failure notifications';

EXEC msdb.dbo.sysmail_add_profileaccount_sp
    @profile_name   = N'ETL Alerts',
    @account_name   = N'ETL Alert Account',
    @sequence_number = 1;

-- Test the configuration
EXEC msdb.dbo.sp_send_dbmail
    @profile_name   = N'ETL Alerts',
    @recipients     = N'Patrick.Dolinger@nscc.ca',
    @subject        = N'Database Mail Test',
    @body           = N'If you receive this, Database Mail is configured correctly.';
```

---

## 4. Hierarchies in Dimensional Models

A **hierarchy** is a structured set of levels in a dimension that defines drill-down and roll-up paths for analysis. Hierarchies are not just organizational conveniences — they are the mechanism by which BI tools enable users to move from summary to detail.

### 4.1 Balanced Hierarchies

A **balanced hierarchy** has the same number of levels in every branch. Every leaf node is at the same depth. These are the most common and the easiest to implement.

**CabotTrail Calendar hierarchy (balanced):**

```
Level 1: FiscalYear      (4 values: 2022/23, 2023/24, 2024/25, 2025/26)
Level 2: FiscalQuarter   (16 values: FQ1 2022/23, FQ2 2022/23, ...)
Level 3: MonthName       (48 values: one per month across all years)
Level 4: FullDate        (4,017 values: one per day)
```

In the CabotTrail DW, this hierarchy is implicit in the `DimDate` column structure. BI tools detect it by naming convention or explicit definition:

```sql
-- Calendar hierarchy columns in DimDate
SELECT DISTINCT
    FiscalYearNumber                AS FiscalYear,
    FiscalYearNumber * 10 + FiscalQuarterNumber AS FiscalYearQuarter,
    FiscalYearNumber * 100 + FiscalPeriodNumber AS FiscalYearMonth,
    FullDate                        AS Day
FROM Dimension.DimDate
ORDER BY FiscalYear, FiscalYearQuarter, FiscalYearMonth, Day;
```

**CabotTrail Geography hierarchy (balanced):**

```
Level 1: CountryName     (typically 1: Canada)
Level 2: ProvinceName    (10 values: NS, NB, ON, ...)
Level 3: CityName        (many values: Halifax, Truro, ...)
```

In `dim.Customer` (datamart), all three levels exist as columns, enabling BI tools to navigate the geography hierarchy:

```sql
-- Geography hierarchy drill-down example
SELECT
    cust.CountryName,
    cust.ProvinceName,
    cust.CityName,
    COUNT(DISTINCT fs.InvoiceID)    AS Invoices,
    SUM(fs.LineTotal)               AS Revenue
FROM CabotTrailOutdoorsSales.fact.Sales fs
INNER JOIN CabotTrailOutdoorsSales.dim.Customer cust
    ON cust.CustomerKey = fs.CustomerKey
GROUP BY
    GROUPING SETS (
        (cust.CountryName),
        (cust.CountryName, cust.ProvinceName),
        (cust.CountryName, cust.ProvinceName, cust.CityName)
    )
ORDER BY cust.CountryName, cust.ProvinceName, cust.CityName;
```

The `GROUPING SETS` clause produces all three levels of the geography hierarchy in a single query — country total, province subtotals, and city detail — which is exactly what a BI tool needs to render a drill-down table.

### 4.2 Ragged (Non-Balanced) Hierarchies

A **ragged hierarchy** has branches of different depths. Some nodes have children; others do not. The employee organizational chart is the classic example — a CEO may have direct reports who are also VPs with managers below them, while other direct reports are individual contributors with no team.

**Handling ragged hierarchies in dimensional models:**

Option 1 — **Bridge table:** A separate table stores the parent-child relationships, allowing recursive navigation. Used when the hierarchy is deep and the depth varies significantly:

```sql
-- Bridge table for a ragged employee hierarchy
CREATE TABLE Dimension.DimEmployeeHierarchy
(
    EmployeeKey         INT     NOT NULL,   -- Child employee
    AncestorKey         INT     NOT NULL,   -- All ancestors (including self)
    DepthDifference     INT     NOT NULL,   -- 0=self, 1=direct parent, 2=grandparent, etc.
    IsDirectParent      BIT     NOT NULL,
    CONSTRAINT PK_DimEmployeeHierarchy
        PRIMARY KEY (EmployeeKey, AncestorKey)
);
```

Option 2 — **Fixed-depth with NULL padding:** If the maximum hierarchy depth is known and small (e.g., 5 levels), store all levels as columns and use NULL for missing levels:

```sql
-- Fixed-depth geography with NULL for missing levels
SELECT
    CountryName,
    ProvinceName,
    SalesTerritory,             -- Territory level (not in all geographies)
    CityName
FROM dim.Customer
-- CityName is always populated; SalesTerritory may be NULL
-- for cities that don't fall into a named territory
```

### 4.3 Product Category Hierarchy in CabotTrail

The CabotTrail product dimension has a two-level hierarchy: Category → Product. The `DimProductCategory` and `DimProduct` relationship represents this:

```sql
-- Product hierarchy: category roll-up
SELECT
    prod.CategoryName,
    prod.ProductName,
    COUNT(*)            AS SalesLines,
    SUM(fs.LineTotal)   AS Revenue
FROM Fact.FactSales fs
INNER JOIN Dimension.DimProduct dp      ON dp.ProductKey       = fs.ProductKey
INNER JOIN Dimension.DimProductCategory pc ON pc.ProductCategoryKey = dp.ProductCategoryKey
GROUP BY
    GROUPING SETS (
        (prod.CategoryName),
        (prod.CategoryName, prod.ProductName)
    )
ORDER BY prod.CategoryName, prod.ProductName;
```

---

## 5. Aggregate Tables and Performance

As fact tables grow, queries that scan millions of rows for summary-level analysis become increasingly slow. **Aggregate tables** pre-compute summaries at higher grains and store them as separate tables, enabling queries to scan thousands of rows instead of millions.

### 5.1 When to Build Aggregate Tables

Aggregate tables are justified when:

1. **Queries are slow:** Key analytical queries take longer than acceptable (typically > 10 seconds for interactive BI)
2. **Query patterns are predictable:** The same aggregation levels are consistently queried (monthly revenue by product category is a common aggregate candidate)
3. **The source fact table is large:** Pre-aggregating a 16-million-row fact table to monthly grain may produce a 50,000-row aggregate — a 320x reduction in scan size
4. **The aggregation is expensive:** Complex measures (rolling averages, percentiles) that are costly to compute at query time benefit from pre-computation

For CabotTrail at 16,359 rows, aggregate tables are not needed for performance. They are built here as a design pattern demonstration — the same pattern applies when the fact table grows to millions of rows.

### 5.2 Building the Monthly Sales Aggregate

```sql
-- Create the aggregate table
USE CabotTrailOutdoorDW;
GO

CREATE TABLE Fact.FactSalesMonthly
(
    -- Grain: one row per calendar month per customer per product
    MonthKey            INT             NOT NULL,   -- YYYYMM (e.g., 202403)
    CustomerKey         INT             NOT NULL,
    ProductKey          INT             NOT NULL,

    -- Pre-aggregated measures
    SalesLineCount      INT             NOT NULL DEFAULT 0,
    InvoiceCount        INT             NOT NULL DEFAULT 0,
    TotalRevenue        DECIMAL(18,2)   NOT NULL DEFAULT 0,
    TotalGrossProfit    DECIMAL(18,2)   NOT NULL DEFAULT 0,
    TotalTaxAmount      DECIMAL(18,2)   NOT NULL DEFAULT 0,
    TotalUnitCost       DECIMAL(18,2)   NOT NULL DEFAULT 0,
    TotalOrderedQty     INT             NOT NULL DEFAULT 0,
    TotalPickedQty      INT             NOT NULL DEFAULT 0,

    -- Derived measure (computed at aggregate level — correct because additive)
    AvgMarginPct        AS CASE
                            WHEN TotalRevenue = 0 THEN 0
                            ELSE ROUND(TotalGrossProfit / TotalRevenue * 100, 2)
                           END PERSISTED,

    CONSTRAINT PK_FactSalesMonthly
        PRIMARY KEY (MonthKey, CustomerKey, ProductKey),
    CONSTRAINT FK_FactSalesMonthly_Customer
        FOREIGN KEY (CustomerKey) REFERENCES Dimension.DimCustomer (CustomerKey),
    CONSTRAINT FK_FactSalesMonthly_Product
        FOREIGN KEY (ProductKey)  REFERENCES Dimension.DimProduct (ProductKey)
);
GO

-- Indexes on common filter columns
CREATE INDEX IX_FactSalesMonthly_MonthKey    ON Fact.FactSalesMonthly (MonthKey);
CREATE INDEX IX_FactSalesMonthly_CustomerKey ON Fact.FactSalesMonthly (CustomerKey);
CREATE INDEX IX_FactSalesMonthly_ProductKey  ON Fact.FactSalesMonthly (ProductKey);
GO
```

### 5.3 Populating the Aggregate

The aggregate is populated from the line-item fact table. This population runs as the final step of the FactSales load — after FactSales is loaded and reconciled:

```sql
-- Populate FactSalesMonthly from FactSales
-- Run after Load_FactSales.dtsx completes

TRUNCATE TABLE Fact.FactSalesMonthly;

INSERT INTO Fact.FactSalesMonthly
(
    MonthKey, CustomerKey, ProductKey,
    SalesLineCount, InvoiceCount,
    TotalRevenue, TotalGrossProfit, TotalTaxAmount, TotalUnitCost,
    TotalOrderedQty, TotalPickedQty
)
SELECT
    d.YearNumber * 100 + d.MonthNumber  AS MonthKey,
    fs.CustomerKey,
    fs.ProductKey,
    COUNT(*)                            AS SalesLineCount,
    COUNT(DISTINCT fs.InvoiceID)        AS InvoiceCount,
    SUM(fs.LineTotal)                   AS TotalRevenue,
    SUM(fs.GrossProfit)                 AS TotalGrossProfit,
    SUM(fs.TaxAmount)                   AS TotalTaxAmount,
    SUM(fs.UnitCost)                    AS TotalUnitCost,
    SUM(fs.OrderedQuantity)             AS TotalOrderedQty,
    SUM(fs.PickedQuantity)              AS TotalPickedQty
FROM    Fact.FactSales fs
INNER JOIN Dimension.DimDate d ON d.DateKey = fs.OrderDateKey
GROUP BY
    d.YearNumber * 100 + d.MonthNumber,
    fs.CustomerKey,
    fs.ProductKey;
GO
```

### 5.4 Query Comparison: Line-Item vs Aggregate

The performance benefit of the aggregate is visible even at CabotTrail's modest scale. At millions of rows the difference is dramatic:

```sql
-- Query using line-item fact (scans 16,359 rows)
SELECT
    d.YearMonthName,
    SUM(fs.LineTotal)   AS Revenue
FROM Fact.FactSales fs
INNER JOIN Dimension.DimDate d ON d.DateKey = fs.OrderDateKey
GROUP BY d.YearMonthName, d.YearNumber, d.MonthNumber
ORDER BY d.YearNumber, d.MonthNumber;

-- Same query using monthly aggregate
-- (scans far fewer rows at scale because one row covers all products per customer per month)
SELECT
    -- Reconstruct YearMonthName from MonthKey integer
    CAST(MonthKey / 100 AS VARCHAR) + '-' +
        RIGHT('0' + CAST(MonthKey % 100 AS VARCHAR), 2)     AS YearMonth,
    SUM(TotalRevenue)   AS Revenue
FROM Fact.FactSalesMonthly
GROUP BY MonthKey
ORDER BY MonthKey;
```

### 5.5 Aggregate Table Maintenance

Aggregate tables are **derived data** — they must be repopulated whenever their source fact table changes. In the master package, the aggregate load runs immediately after the fact load and its reconciliation:

```
[Load_FactSales.dtsx] → (success) → [Load_FactSalesMonthly: TRUNCATE + INSERT] → (success) → [Reconcile Aggregate]
```

**Reconciliation for the aggregate:**

```sql
-- Aggregate reconciliation: total revenue must match between line-item and aggregate
SELECT
    'Line-item total'   AS Source,
    SUM(LineTotal)      AS TotalRevenue
FROM Fact.FactSales
UNION ALL
SELECT
    'Aggregate total',
    SUM(TotalRevenue)
FROM Fact.FactSalesMonthly;
-- Both values must be identical
```

---

## 6. The SSIS Catalog: Deployment and Configuration

The SSIS Catalog (`SSISDB`) is the production deployment platform for SSIS packages. It provides deployment management, environment-based configuration, and integrated monitoring.

### 6.1 Deploying to the SSIS Catalog

**From Visual Studio (SSDT):**

1. Right-click the SSIS project in Solution Explorer → Deploy
2. Select destination: `SQL Server` → enter server name → select the `SSISDB` catalog
3. Select or create a folder: `CabotTrailETL`
4. Review the deployment summary → Deploy

**From T-SQL (for automated deployment pipelines):**

```sql
-- Deploy an ISPAC (Integration Services Project Archive) file
USE SSISDB;

DECLARE @ProjectBinary VARBINARY(MAX);
DECLARE @Operation     INT;

-- Read the ISPAC file (requires xp_cmdshell or pre-staged binary)
-- Typically automated via PowerShell in CI/CD pipelines

EXEC catalog.deploy_project
    @folder_name    = N'CabotTrailETL',
    @project_name   = N'CabotTrailETL',
    @project_stream = @ProjectBinary,
    @operation_id   = @Operation OUTPUT;
```

### 6.2 SSIS Environments for Configuration

The most powerful feature of the SSIS Catalog is **environment-based configuration**. An SSIS environment is a named collection of variables — essentially a configuration profile that can be applied to a package execution without modifying the package itself.

**Create environments for each deployment tier:**

```sql
-- Create the Production environment
EXEC catalog.create_environment
    @folder_name        = N'CabotTrailETL',
    @environment_name   = N'Production',
    @environment_description = N'Production connection strings and settings';

-- Add variables to the environment
EXEC catalog.create_environment_variable
    @folder_name        = N'CabotTrailETL',
    @environment_name   = N'Production',
    @variable_name      = N'SourceConnectionString',
    @sensitive          = TRUE,         -- Encrypted at rest
    @value              = N'Data Source=PROD-SQL01;Initial Catalog=CabotTrailOutdoor;Integrated Security=SSPI;',
    @data_type          = N'String';

EXEC catalog.create_environment_variable
    @folder_name        = N'CabotTrailETL',
    @environment_name   = N'Production',
    @variable_name      = N'TargetConnectionString',
    @sensitive          = TRUE,
    @value              = N'Data Source=PROD-SQL01;Initial Catalog=CabotTrailOutdoorDW;Integrated Security=SSPI;',
    @data_type          = N'String';

EXEC catalog.create_environment_variable
    @folder_name        = N'CabotTrailETL',
    @environment_name   = N'Production',
    @variable_name      = N'AlertEmail',
    @sensitive          = FALSE,
    @value              = N'Patrick.Dolinger@nscc.ca',
    @data_type          = N'String';
```

```sql
-- Reference the environment from the project
EXEC catalog.create_environment_reference
    @folder_name        = N'CabotTrailETL',
    @project_name       = N'CabotTrailETL',
    @environment_name   = N'Production',
    @environment_folder_name = N'CabotTrailETL',
    @reference_type     = N'A';     -- 'A' = Absolute reference (same folder)
```

**Create a parallel environment for Test:**

```sql
EXEC catalog.create_environment
    @folder_name        = N'CabotTrailETL',
    @environment_name   = N'Test',
    @environment_description = N'Test environment — points to test databases';

EXEC catalog.create_environment_variable
    @folder_name        = N'CabotTrailETL',
    @environment_name   = N'Test',
    @variable_name      = N'SourceConnectionString',
    @sensitive          = TRUE,
    @value              = N'Data Source=TEST-SQL01;Initial Catalog=CabotTrailOutdoor_Test;Integrated Security=SSPI;',
    @data_type          = N'String';
-- (repeat for all variables with test values)
```

Now the same deployed packages can execute against Production or Test simply by selecting a different environment reference — no package modification required.

---

## 7. Environment Promotion: DEV → Test → QA → Production

Professional ETL development follows a structured promotion path. Code is developed in one environment and promoted through increasingly production-like environments, with testing and approval at each stage.

### 7.1 The Four-Environment Model

| Environment | Purpose | Data | Who uses it | Promotion requires |
|---|---|---|---|---|
| **DEV** | Build and debug packages | Subset / sample data | ETL developer | Developer self-review |
| **Test** | Verify packages against complete data | Full copy of production data | ETL developer + QA analyst | All reconciliation tests pass |
| **QA** | Business validation against production-like data | Production data copy (recent) | QA analyst + business users | Business sign-off |
| **Production** | Live system | Live OLTP source | Automated (Agent job) | Change management approval |

### 7.2 What Changes Between Environments

The SSIS packages themselves do not change between environments — they are the same `.dtsx` files. What changes are the environment variables in the SSIS Catalog:

| Variable | DEV | Test | QA | Production |
|---|---|---|---|---|
| `SourceConnectionString` | Local DEV SQL | TEST-SQL01 | QA-SQL01 | PROD-SQL01 |
| `TargetConnectionString` | Local DEV DW | TEST-DW | QA-DW | PROD-DW |
| `AlertEmail` | developer@nscc.ca | dev-team@nscc.ca | qa-team@nscc.ca | ops@nscc.ca |
| `LoadBatchSize` | 1,000 | 10,000 | 100,000 | Unlimited |

### 7.3 The Promotion Checklist

Each promotion between environments requires completing a checklist:

```
PROMOTION CHECKLIST: Test → QA
Project: CabotTrailETL v1.3
Date: [Date]
Promoted by: [Name]
Approved by: [Name]

Pre-promotion:
[ ] All reconciliation tests pass in Test environment
[ ] ETL.TestResults shows no failures for latest Test run
[ ] Data dictionary updated to reflect any schema changes in this version
[ ] S2T mapping updated and version-incremented
[ ] Process flow diagrams updated if pipeline structure changed
[ ] Change log entry added to ETL Design Document

Deployment:
[ ] SSIS Catalog environment variables updated for QA
[ ] ISPAC deployed to QA server
[ ] QA Agent job updated to reference new environment
[ ] Initial manual test execution run — packages complete without errors

Post-deployment validation:
[ ] All reconciliation tests pass in QA environment
[ ] Row counts match Test environment results
[ ] Revenue reconciliation passes
[ ] Orphan check returns 0
[ ] Business user spot-check of sample data completed
[ ] Sign-off obtained from: [Business stakeholder name]

Rollback plan:
[ ] Previous ISPAC version retained and labelled v1.2_backup
[ ] Rollback procedure documented: redeploy v1.2_backup, revert environment variables
```

### 7.4 Automating Promotion with PowerShell

Production-grade ETL shops automate environment promotion using PowerShell or CI/CD pipelines (Azure DevOps, GitHub Actions):

```powershell
# PowerShell: Deploy SSIS project to a specified environment
param(
    [string]$ServerName,
    [string]$FolderName    = "CabotTrailETL",
    [string]$ProjectName   = "CabotTrailETL",
    [string]$IspacPath     = ".\bin\Development\CabotTrailETL.ispac",
    [string]$Environment   = "Production"
)

# Load the SSIS assembly
[System.Reflection.Assembly]::LoadWithPartialName("Microsoft.SqlServer.Management.IntegrationServices") | Out-Null

$sqlConnection = New-Object System.Data.SqlClient.SqlConnection
$sqlConnection.ConnectionString = "Data Source=$ServerName;Initial Catalog=SSISDB;Integrated Security=SSPI;"
$sqlConnection.Open()

$integrationServices = New-Object Microsoft.SqlServer.Management.IntegrationServices.IntegrationServices($sqlConnection)
$catalog = $integrationServices.Catalogs["SSISDB"]
$folder  = $catalog.Folders[$FolderName]

# Read the ISPAC file
$projectBytes = [System.IO.File]::ReadAllBytes($IspacPath)

# Deploy
Write-Host "Deploying $ProjectName to $ServerName/$FolderName (Environment: $Environment)..."
$folder.DeployProject($ProjectName, $projectBytes)

Write-Host "Deployment complete. Validating..."
$project = $folder.Projects[$ProjectName]
$project.Validate()
Write-Host "Validation: $($project.ValidationStatus)"
```

---

## 8. ETL Performance Tuning

Performance problems in ETL manifest in two ways: the load takes too long (window violation) or individual transforms are slow (bottleneck). Diagnosing and resolving each requires different techniques.

### 8.1 Identifying the Bottleneck

The SSIS Catalog execution statistics reveal where time is spent:

```sql
-- Time spent in each Data Flow component for the most recent execution
SELECT
    eds.package_name,
    eds.task_name,
    eds.dataflow_path_id_string     AS Component,
    eds.rows_sent,
    -- Rows per second (approximate throughput)
    CAST(eds.rows_sent AS DECIMAL)
        / NULLIF(DATEDIFF(SECOND, e.start_time, e.end_time), 0) AS RowsPerSecond
FROM    SSISDB.catalog.execution_data_statistics eds
INNER JOIN SSISDB.catalog.executions e ON e.execution_id = eds.execution_id
WHERE   e.execution_id = (SELECT MAX(execution_id) FROM SSISDB.catalog.executions)
ORDER BY eds.rows_sent DESC;
```

Low rows-per-second for a specific component identifies the bottleneck. Common bottleneck sources:

| Symptom | Likely cause | Resolution |
|---|---|---|
| OLE DB Source slow | Missing index on source WHERE clause | Add index on source table |
| Lookup transformation slow | Repeated database calls (no-cache mode) | Switch to Full Cache mode |
| OLE DB Destination slow | Row-by-row insert mode | Switch to Fast Load mode |
| Sort transformation slow | Sorting all rows in memory | Pre-sort in source SQL ORDER BY |
| Script component slow | Complex C# per-row logic | Rewrite as set-based SQL or Derived Column |

### 8.2 Source Query Optimization

The extract query is the first opportunity for performance improvement. Every join, filter, and computation in the source query runs against the OLTP — an operational system that must remain responsive for business users.

```sql
-- Index the OLTP source for ETL extraction performance
-- (Run during maintenance window on the OLTP server)

-- Index for incremental extraction by LastEditedWhen
CREATE NONCLUSTERED INDEX IX_InvoiceLines_LastEdited
    ON CabotTrailOutdoor.Sales.InvoiceLines (LastEditedWhen)
    INCLUDE (InvoiceID, ProductID, Quantity, UnitPrice, LineTotal, TaxAmount);

-- Index for the M:M category join (primary category lookup)
CREATE NONCLUSTERED INDEX IX_ProductCategoryAssignments_ProductID
    ON CabotTrailOutdoor.Inventory.ProductCategoryAssignments (ProductID)
    INCLUDE (ProductCategoryID);
```

### 8.3 Destination Write Optimization

For large fact table loads, destination write performance is often the primary bottleneck.

**Minimize index maintenance during load:**

```sql
-- Option 1: Disable non-clustered indexes before load, rebuild after
-- (Effective for full-reload patterns; not for incremental)

-- Before load:
ALTER INDEX IX_FactSales_CustomerKey   ON Fact.FactSales DISABLE;
ALTER INDEX IX_FactSales_ProductKey    ON Fact.FactSales DISABLE;
ALTER INDEX IX_FactSales_OrderDateKey  ON Fact.FactSales DISABLE;

-- [Run SSIS load]

-- After load:
ALTER INDEX IX_FactSales_CustomerKey   ON Fact.FactSales REBUILD;
ALTER INDEX IX_FactSales_ProductKey    ON Fact.FactSales REBUILD;
ALTER INDEX IX_FactSales_OrderDateKey  ON Fact.FactSales REBUILD;
```

**Minimize logging with minimal logging:**

```sql
-- Use minimal logging for bulk inserts (reduces transaction log growth)
-- Requires: target table uses SIMPLE or BULK-LOGGED recovery model
--           and table has no non-clustered indexes (or they are disabled)
ALTER DATABASE CabotTrailOutdoorDW SET RECOVERY BULK_LOGGED;

-- [Run SSIS load with TABLOCK hint on destination]

ALTER DATABASE CabotTrailOutdoorDW SET RECOVERY SIMPLE;
```

### 8.4 Parallelism in SSIS

Multiple Data Flow Tasks can execute in parallel within the same package or across packages in the master package. Configuring the correct degree of parallelism improves throughput without overloading the server.

```
Master Package Control Flow:
┌─────────────────────────────────────────────────────────────────┐
│  Sequence Container: Tier 1 Dimensions                          │
│  MaxConcurrentExecutables = 5  (all five run simultaneously)    │
│                                                                 │
│  [DimDate]  [DimDeliveryMethod]  [DimTransactionType]           │
│  [DimPaymentMethod]  [DimProductCategory]                       │
└─────────────────────────────────────────────────────────────────┘
```

SSIS package-level parallelism is configured via the `MaxConcurrentExecutables` property on the package or container. Setting it to -1 (default) uses the number of logical processors; setting it to 1 forces sequential execution.

**Important:** Parallelism increases both throughput and resource contention. Monitor SQL Server CPU, memory, and I/O during parallel loads to ensure the server is not overloaded. For CabotTrail's small data volumes, sequential execution is fine and simpler to debug.

### 8.5 Connection Pooling

Opening and closing database connections is expensive. SSIS reuses connections within a package through its connection manager pooling. Best practices:

- Use **project-level connection managers** (shared across all packages in the project) rather than package-level ones (recreated for each package)
- Set `RetainSameConnection = True` on connection managers where possible (maintains the connection across multiple tasks in the same package)
- Do not create connection managers inside Foreach Loop containers (creates and destroys connections on each iteration)

---

## 9. ETL Versioning and Rollback

Every production ETL system needs a strategy for versioning packages and rolling back changes that cause problems.

### 9.1 Version Control for SSIS Packages

SSIS packages (`.dtsx` files) are XML files. They can be stored in any source control system — Git, Azure DevOps, SVN. The standard practice:

```
etl-cabot-trail/                    ← Git repository root
├── CabotTrailETL.dtproj            ← SSIS project file
├── Master_DW_Load.dtsx             ← Packages
├── Load_DimCustomer.dtsx
├── Load_DimProduct.dtsx
├── Load_FactSales.dtsx
├── [... all packages ...]
└── docs/
    ├── ETL_DesignDocument_v1.3.pdf
    └── S2T_Mapping_v1.3.xlsx
```

**Git commit discipline for ETL:**
- One commit per logical change (adding a column, changing a derivation rule)
- Commit message references the business change: "Add PromotionCode to FactSales per CR-047"
- Tag release versions: `git tag v1.3-production-2027-02-12`

### 9.2 The SSIS Catalog's Built-In Versioning

The SSIS Catalog retains previous versions of deployed projects. When a new ISPAC is deployed, the previous version is retained (up to the configured retention limit):

```sql
-- View deployment history for the project
SELECT
    pv.project_version_lsn,
    pv.name             AS ProjectName,
    pv.description,
    pv.deployed_by_name,
    pv.last_deployed_time
FROM    SSISDB.catalog.projects p
INNER JOIN SSISDB.catalog.project_versions pv
    ON pv.project_id = p.project_id
WHERE   p.name = N'CabotTrailETL'
ORDER BY pv.last_deployed_time DESC;
```

```sql
-- Restore a previous version (rollback)
EXEC SSISDB.catalog.restore_project
    @folder_name    = N'CabotTrailETL',
    @project_name   = N'CabotTrailETL',
    @project_version_lsn = [version_lsn_from_above];
```

### 9.3 The Rollback Decision

A rollback is warranted when a deployment causes:
- Reconciliation test failures that cannot be quickly resolved
- Business users reporting incorrect data
- Performance degradation beyond the load window

The rollback decision should be made within a defined time window — typically 2 hours after deployment. If the issue is not resolved within the window, roll back and fix forward.

**Rollback procedure:**

```
1. Stop the current Agent job if running:
   EXEC msdb.dbo.sp_stop_job @job_name = 'CabotTrail_DW_Nightly_Load';

2. Restore previous SSIS package version from Catalog:
   EXEC SSISDB.catalog.restore_project
       @folder_name = 'CabotTrailETL',
       @project_name = 'CabotTrailETL',
       @project_version_lsn = [previous_version_lsn];

3. If schema changes were made (new columns, tables):
   -- Revert the schema change
   ALTER TABLE Fact.FactSales DROP COLUMN PromotionCode;
   -- or restore from backup

4. Re-run the load:
   EXEC msdb.dbo.sp_start_job @job_name = 'CabotTrail_DW_Nightly_Load';

5. Verify: all reconciliation tests pass with previous version's results.
```

### 9.4 Schema Change Management

When ETL changes require schema changes (new columns, new tables), the schema change and the package change must be deployed together — and rolled back together:

```sql
-- Pre-deployment schema migration script
-- Run BEFORE deploying the new SSIS packages

-- Step 1: Add new column (nullable first for safe deployment)
ALTER TABLE Fact.FactSales
ADD PromotionCode NVARCHAR(20) NULL;

-- [Deploy new SSIS packages]
-- [Run reconciliation tests]

-- Step 2: If tests pass, enforce NOT NULL constraint after data is loaded
-- (Cannot add NOT NULL constraint on existing rows until they have values)
ALTER TABLE Fact.FactSales
ALTER COLUMN PromotionCode NVARCHAR(20) NOT NULL;

-- Rollback schema migration (if new packages fail):
ALTER TABLE Fact.FactSales DROP COLUMN PromotionCode;
```

The pattern of adding columns as nullable first, then constraining after the load, is standard practice for zero-downtime schema changes in production environments.

---

## 10. Chapter Summary

- **Production ETL** is a service with a contract covering freshness, completeness, accuracy, availability, and notification. Every operational practice in this chapter exists to fulfil one or more elements of that contract.

- **SQL Server Agent** schedules SSIS packages through jobs with defined schedules, multi-step execution, and email notifications on failure. On-demand execution is available through stored procedure wrappers.

- **Three layers of monitoring** are necessary: job-level (Agent history), package-level (SSIS Catalog), and business-level (ETL.TestResults). An operational dashboard query combines all three into a single status view.

- **Balanced hierarchies** (calendar, geography) are implemented as multiple columns in dimension tables. **Ragged hierarchies** require bridge tables or fixed-depth NULL-padded columns. `GROUPING SETS` enables multi-level aggregation in a single query.

- **Aggregate tables** pre-compute summaries at higher grains to accelerate common queries. They must be repopulated whenever the source fact changes and reconciled against the source fact.

- The **SSIS Catalog** provides deployment management, environment-based configuration, and execution monitoring. SSIS environments store connection strings and parameters separately from packages, enabling the same packages to run in DEV, Test, QA, and Production without modification.

- **Environment promotion** follows a four-stage model (DEV → Test → QA → Production) with a formal checklist at each stage, including reconciliation test validation and business sign-off before Production.

- **Performance tuning** starts with identifying the bottleneck using Catalog execution statistics. Common interventions include source query indexing, Lookup cache mode, destination fast load, index disabling during bulk loads, and parallelism configuration.

- **Versioning and rollback** require source control for packages, SSIS Catalog version retention, and a defined rollback procedure. Schema changes must be deployed and rolled back alongside their corresponding package changes.

---

## 11. Review Questions

1. Explain the three layers of ETL monitoring and describe a scenario where all three are needed together. Specifically: what would a successful job with a failed business-level test look like, and how would you detect it?

2. Write a SQL Server Agent job definition (using `sp_add_job`, `sp_add_jobstep`, and `sp_add_schedule`) for a job that runs `Load_DimCustomer.dtsx` every Sunday at midnight. Configure it to email `Patrick.Dolinger@nscc.ca` on failure.

3. The CabotTrail nightly load currently runs the five Tier 1 dimension loads sequentially. A colleague proposes running them in parallel within a Sequence Container. What are the performance benefits, and what conditions must be true for this parallelism to be safe?

4. `Fact.FactSalesMonthly` is an aggregate of `Fact.FactSales` at monthly grain. A new sale is added to `Fact.FactSales` during today's nightly load. Describe what must happen to keep `Fact.FactSalesMonthly` consistent with `Fact.FactSales`. Write the reconciliation query that verifies consistency.

5. Explain why SSIS environment variables are essential for the DEV → Production promotion model. What specific problem would arise if connection strings were hardcoded in the packages instead?

6. The production nightly load fails at 2:47 AM. Your monitoring query shows that `Load_FactSales.dtsx` succeeded (16,359 rows loaded) but the revenue reconciliation test failed — the DW shows $3,200 less revenue than the source. Describe your diagnostic process step by step.

7. A new `PromotionCode` column needs to be added to `Fact.FactSales`. Describe the complete deployment sequence: schema migration, package deployment, testing, and the rollback procedure if tests fail.

8. The `FactSales` load takes 45 minutes and the batch window is 2 hours. A performance analysis shows that the OLE DB Destination is the bottleneck, writing at 200 rows/second. The table has five non-clustered indexes. Describe two specific interventions that could improve write performance and explain why each works.

---

## 🔍 Deeper Dive

### Going Further with ETL Administration

#### SQL Server Agent Under the Hood

SQL Server Agent runs as a Windows service (`SQLSERVERAGENT`) and communicates with the SQL Server engine through the `msdb` system database. Understanding the `msdb` schema enables sophisticated monitoring and automation:

Key `msdb` tables:
- `dbo.sysjobs` — job definitions
- `dbo.sysjobsteps` — individual step definitions
- `dbo.sysjobschedules` — schedule-to-job mappings
- `dbo.sysjobhistory` — execution history (purged automatically based on retention settings)
- `dbo.sysalerts` — alert definitions (CPU, disk space, SQL error numbers)
- `dbo.sysoperators` — notification recipients

The history retention setting is important for monitoring — by default Agent retains only the last 1,000 history rows per job. For long-running production systems, increase the retention or export history to a custom audit table before it is purged:

```sql
-- Configure Agent history retention (requires sysadmin)
EXEC msdb.dbo.sp_set_sqlagent_properties
    @jobhistory_max_rows            = 10000,    -- Per job
    @jobhistory_max_rows_per_job    = 1000;
```

Microsoft documentation:
[SQL Server Agent](https://learn.microsoft.com/en-us/sql/ssms/agent/sql-server-agent)

#### GROUPING SETS, ROLLUP, and CUBE

The `GROUPING SETS` clause used in section 4.1 is one of three SQL extension operators for multi-level aggregation:

**`GROUPING SETS`:** Explicit list of grouping combinations. Most flexible — you specify exactly which combinations you want.

**`ROLLUP`:** Automatically generates a hierarchy of aggregations from right to left in the column list. `GROUP BY ROLLUP(Country, Province, City)` produces City totals, Province totals, Country totals, and a grand total — same as a balanced hierarchy drill-up.

**`CUBE`:** Generates all possible combinations of the columns. `GROUP BY CUBE(Country, Province, City)` produces every combination including City-only, Province-only, Country-only subtotals — useful for cross-tabulation but can produce many combinations for large column lists.

```sql
-- ROLLUP: hierarchy roll-up (Country → Province → City)
SELECT
    ISNULL(CountryName, '(All Countries)')   AS Country,
    ISNULL(ProvinceName, '(All Provinces)')  AS Province,
    ISNULL(CityName, '(All Cities)')         AS City,
    SUM(LineTotal)                           AS Revenue
FROM CabotTrailOutdoorsSales.fact.Sales fs
INNER JOIN CabotTrailOutdoorsSales.dim.Customer cust
    ON cust.CustomerKey = fs.CustomerKey
GROUP BY ROLLUP (cust.CountryName, cust.ProvinceName, cust.CityName)
ORDER BY Country, Province, City;
```

Microsoft documentation:
[GROUP BY ROLLUP, CUBE, GROUPING SETS](https://learn.microsoft.com/en-us/sql/t-sql/queries/select-group-by-transact-sql)

#### Columnstore Indexes for Large Fact Tables

For fact tables with tens of millions of rows, the performance difference between row-store and column-store indexing is dramatic. **Columnstore indexes** store data column by column rather than row by row, enabling extremely efficient compression and vectorized processing for analytical aggregations.

```sql
-- Create a clustered columnstore index on a large fact table
-- (Replaces the clustered row-store index)
CREATE CLUSTERED COLUMNSTORE INDEX CCI_FactSales
    ON Fact.FactSales
    WITH (DROP_EXISTING = OFF, ONLINE = OFF);

-- Or add a non-clustered columnstore as an additional index
-- (Leaves the existing clustered row-store index in place)
CREATE NONCLUSTERED COLUMNSTORE INDEX NCCI_FactSales_Analytics
    ON Fact.FactSales
    (OrderDateKey, CustomerKey, ProductKey, LineTotal, GrossProfit);
```

For CabotTrail at 16,359 rows, columnstore indexing provides no benefit — the overhead of index maintenance exceeds any query acceleration. At 10+ million rows, a columnstore index on the fact table typically reduces analytical query time by 10–100×.

Microsoft documentation:
[Columnstore Indexes — SQL Server](https://learn.microsoft.com/en-us/sql/relational-databases/indexes/columnstore-indexes-overview)

#### CI/CD for ETL: Automating the Promotion Pipeline

Modern DevOps practices can be applied to ETL development through **Continuous Integration / Continuous Deployment (CI/CD)** pipelines. Tools like Azure DevOps, GitHub Actions, and Jenkins can automate the promotion process described in section 7.4:

**A typical CI/CD pipeline for SSIS:**

```
Developer pushes code → Git
    ↓
CI Pipeline (Azure DevOps):
  1. Build the SSIS project → generate ISPAC
  2. Run unit tests (SQL scripts against DEV database)
  3. Deploy ISPAC to Test environment
  4. Run integration tests (full reconciliation suite against Test)
  5. If all tests pass → create release candidate

CD Pipeline (manual approval gate):
  6. Approve promotion to QA
  7. Deploy to QA
  8. Run QA validation tests
  9. Business user sign-off
 10. Approve promotion to Production
 11. Deploy to Production
 12. Run production reconciliation tests
 13. Notify on success/failure
```

This pipeline eliminates manual deployment steps and enforces the promotion checklist automatically. Any failed test at any stage blocks the promotion.

Microsoft documentation:
[Build and deploy SSIS packages — Azure DevOps](https://learn.microsoft.com/en-us/azure/devops/pipelines/targets/azure-sqldb)

#### Partition Switching for Large Fact Tables

For very large fact tables (hundreds of millions of rows), **table partitioning** with **partition switching** enables near-instantaneous load operations by staging data in a separate table and then switching the partition into the main table in a metadata-only operation.

```sql
-- Create a partitioned fact table (partition by year)
CREATE PARTITION FUNCTION pf_FactSales_Year (INT)
AS RANGE RIGHT FOR VALUES (20220101, 20230101, 20240101, 20250101);

CREATE PARTITION SCHEME ps_FactSales_Year
AS PARTITION pf_FactSales_Year
TO ([PRIMARY], [PRIMARY], [PRIMARY], [PRIMARY], [PRIMARY]);

CREATE TABLE Fact.FactSales_Partitioned
(
    [... same columns as FactSales ...]
    OrderDateKey INT NOT NULL
) ON ps_FactSales_Year(OrderDateKey);

-- Load a staging table for the new period, then switch it in
-- (O(1) metadata operation — instant regardless of row count)
ALTER TABLE Fact.FactSales_Staging
    SWITCH TO Fact.FactSales_Partitioned PARTITION 5;  -- 2025 partition
```

Partition switching is the gold standard for large-scale fact table loads — it enables daily or even hourly incremental loads of billions of rows with minimal impact on query performance.

Microsoft documentation:
[Partitioned Tables and Indexes](https://learn.microsoft.com/en-us/sql/relational-databases/partitions/partitioned-tables-and-indexes)

---

### Industry Perspectives

#### Kimball on ETL Operations

Kimball dedicates significant attention in *The Data Warehouse ETL Toolkit* to the operational aspects of ETL that this chapter covers. His perspective on monitoring is particularly relevant:

> *"The ETL system must be fully monitored. Operations staff should never be in a position where they are unsure whether last night's load ran successfully. A complete monitoring infrastructure — scheduler, error notification, reconciliation reporting — is not optional. It is part of the ETL system."*

Kimball treats monitoring not as an add-on but as a core deliverable of the ETL system — the same status as the dimension and fact loads themselves.

Kimball, R., & Caserta, J. (2004). *The Data Warehouse ETL Toolkit*. Wiley. — Chapter 14 covers ETL operations, scheduling, and monitoring.

---

### References and Further Reading

1. Kimball, R., & Caserta, J. (2004). *The Data Warehouse ETL Toolkit*. Wiley. — Chapter 14 covers ETL operations and scheduling.

2. Microsoft. (2024). *SQL Server Agent*. [https://learn.microsoft.com/en-us/sql/ssms/agent/sql-server-agent](https://learn.microsoft.com/en-us/sql/ssms/agent/sql-server-agent)

3. Microsoft. (2024). *SSIS Catalog*. [https://learn.microsoft.com/en-us/sql/integration-services/catalog/ssis-catalog](https://learn.microsoft.com/en-us/sql/integration-services/catalog/ssis-catalog)

4. Microsoft. (2024). *GROUP BY ROLLUP, CUBE, GROUPING SETS*. [https://learn.microsoft.com/en-us/sql/t-sql/queries/select-group-by-transact-sql](https://learn.microsoft.com/en-us/sql/t-sql/queries/select-group-by-transact-sql)

5. Microsoft. (2024). *Columnstore Indexes Overview*. [https://learn.microsoft.com/en-us/sql/relational-databases/indexes/columnstore-indexes-overview](https://learn.microsoft.com/en-us/sql/relational-databases/indexes/columnstore-indexes-overview)

6. Microsoft. (2024). *Partitioned Tables and Indexes*. [https://learn.microsoft.com/en-us/sql/relational-databases/partitions/partitioned-tables-and-indexes](https://learn.microsoft.com/en-us/sql/relational-databases/partitions/partitioned-tables-and-indexes)

7. Microsoft. (2024). *Deploy SSIS Projects and Packages*. [https://learn.microsoft.com/en-us/sql/integration-services/packages/deploy-integration-services-ssis-projects-and-packages](https://learn.microsoft.com/en-us/sql/integration-services/packages/deploy-integration-services-ssis-projects-and-packages)

8. Microsoft. (2024). *Database Mail*. [https://learn.microsoft.com/en-us/sql/relational-databases/database-mail/database-mail](https://learn.microsoft.com/en-us/sql/relational-databases/database-mail/database-mail)

9. Microsoft. (2024). *Data Flow Performance Features*. [https://learn.microsoft.com/en-us/sql/integration-services/data-flow/data-flow-performance-features](https://learn.microsoft.com/en-us/sql/integration-services/data-flow/data-flow-performance-features)

10. Itzik Ben-Gan. (2015). *T-SQL Querying*. Microsoft Press. — Chapters 7–8 cover GROUPING SETS, ROLLUP, and CUBE in depth with performance guidance.

11. Fritchey, G. (2018). *SQL Server Query Performance Tuning* (5th ed.). Apress. — Comprehensive guide to SQL Server performance analysis and tuning, including index strategies for analytical workloads.

---

*Previous chapter: [Chapter 7 — Advanced ETL: MERGE, Slowly Changing Dimensions, and Multiple Sources](../chapter-07-advanced-etl/README.md)*

*Next chapter: [Chapter 9 — Putting It Together: The Complete ETL Pipeline](../chapter-09-complete-pipeline/README.md)*

---

> **ETL for Business Intelligence** | © Patrick Dolinger, NSCC Institute of Technology
> [CC BY 4.0](https://creativecommons.org/licenses/by/4.0/) — Share and adapt freely with attribution
