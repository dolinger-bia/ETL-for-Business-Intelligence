# Chapter 5: Testing, Data Quality, and Governance

> **ETL for Business Intelligence**
> *A practical guide to data provisioning, dimensional modelling, and pipeline design*
>
> © Patrick Dolinger, NSCC Institute of Technology
> Licensed under [Creative Commons Attribution 4.0 International (CC BY 4.0)](https://creativecommons.org/licenses/by/4.0/)
> You are free to share and adapt this material for any purpose, provided appropriate credit is given.

---

## Chapter Overview

Chapters 3 and 4 covered the design and implementation of ETL pipelines. A pipeline that runs without errors is not the same as a pipeline that produces *correct* results. This chapter addresses the discipline that bridges that gap: testing and data quality governance.

An ETL system without a formal test suite is an assertion — "I believe this is correct." An ETL system with a formal test suite is a verified claim — "this is correct, and here is the evidence." In a business intelligence context, the difference matters enormously. Analysts, managers, and executives make decisions based on data from the warehouse. If that data is wrong — even subtly, even occasionally — the decisions built on it are compromised.

Data governance provides the organizational framework that makes testing sustainable: the policies, standards, and accountability structures that ensure data quality is maintained not just at initial load but continuously, through system changes, source data changes, and business rule changes.

By the end of this chapter you will be able to:

- Describe the five levels of ETL testing and explain when each is applied
- Build a formal ETL test suite using the `ETL.TestResults` table pattern
- Implement automated reconciliation tests that run after every load
- Distinguish between data governance and data quality, and explain the relationship between them
- Describe the key elements of a data governance framework for a BI environment
- Implement SSIS logging and connect it to a monitoring strategy
- Explain the role of metadata in ETL governance

---

## Table of Contents

1. [Why ETL Testing Is Different](#1-why-etl-testing-is-different)
2. [The Five Levels of ETL Testing](#2-the-five-levels-of-etl-testing)
3. [Building a Formal Test Suite](#3-building-a-formal-test-suite)
4. [Reconciliation Testing in Depth](#4-reconciliation-testing-in-depth)
5. [Data Quality in Production](#5-data-quality-in-production)
6. [SSIS Logging and Monitoring](#6-ssis-logging-and-monitoring)
7. [Introduction to Data Governance](#7-introduction-to-data-governance)
8. [Metadata Management](#8-metadata-management)
9. [Chapter Summary](#9-chapter-summary)
10. [Review Questions](#10-review-questions)
11. [🔍 Deeper Dive](#-deeper-dive)

---

## 1. Why ETL Testing Is Different

Testing an ETL system presents challenges that do not arise in application software testing. Understanding these differences shapes the testing strategy.

### 1.1 The Output Is Data, Not Behaviour

Application software testing verifies that a function returns the correct value or that a UI element behaves correctly. ETL testing verifies that millions of data rows have been transformed correctly according to business rules. There is no return value to assert against — there are rows in a table that must collectively satisfy a set of conditions.

This means ETL tests are fundamentally **set-based**: they operate on aggregations, distributions, and relationships across the entire dataset, not on individual records.

### 1.2 The Source Data Changes

An application can be tested against a fixed, controlled input. ETL source data changes with every load — new orders are placed, new customers are created, prices are updated. Tests that passed yesterday must pass again today against new data. The test suite must be designed to accommodate evolving data without requiring constant maintenance.

The solution is to write tests that verify **structural properties** (the relationship between source and target counts, the referential integrity of FK relationships, the mathematical correctness of derivations) rather than specific values. A test that says "revenue must equal $6,350,582.50" breaks every time a new sale is loaded. A test that says "DW revenue must equal source revenue" is valid for the life of the system.

### 1.3 Failures Can Be Silent

An application that crashes announces its failure. An ETL pipeline that silently drops 3% of rows — because a lookup found no match and the no-match rows were not redirected to an error output — delivers a subtly wrong data warehouse with no error messages. Analysts will eventually notice that numbers are slightly off, but tracing the issue back to a silent lookup failure may take days.

The test suite is the mechanism that makes silent failures audible. If every load is followed by a reconciliation test that compares source and target counts, a 3% row loss is detected immediately.

### 1.4 The "Correct Answer" Is a Business Agreement

In application testing, correct behaviour is defined by a specification. In ETL testing, correct data is defined by a business agreement — the S2T mapping from Chapter 3. This agreement must be documented and maintained. If the business changes its mind about how `SalesTerritory` is derived, the test that validates `SalesTerritory` values must be updated along with the ETL code.

This coupling between business rules, ETL implementation, and tests is why the S2T mapping is a living document: it is the single source of truth that keeps all three in alignment.

---

## 2. The Five Levels of ETL Testing

ETL testing is structured in levels that correspond to the stages of development and deployment. Each level catches different categories of defects.

### 2.1 Unit Testing

**Unit testing** validates a single transformation in isolation. For ETL, a unit test verifies that one specific transformation rule produces the correct output for a known input.

**Example:** Testing the `SalesTerritory` derivation rule for `DimCustomer`.

```sql
-- Unit test: SalesTerritory derivation
-- Input: ProvinceCode values from source
-- Expected output: corresponding SalesTerritory values

-- Build a controlled test dataset
WITH TestCases AS (
    SELECT 'NS' AS ProvinceCode, 'Atlantic — Nova Scotia'     AS ExpectedTerritory UNION ALL
    SELECT 'NB',                 'Atlantic — New Brunswick'                          UNION ALL
    SELECT 'ON',                 'Ontario'                                           UNION ALL
    SELECT 'AB',                 'Alberta'                                           UNION ALL
    SELECT 'XX',                 'Other'   -- Unknown province
),
ActualResults AS (
    SELECT
        ProvinceCode,
        CASE ProvinceCode
            WHEN 'NS' THEN 'Atlantic — Nova Scotia'
            WHEN 'NB' THEN 'Atlantic — New Brunswick'
            WHEN 'PE' THEN 'Atlantic — PEI'
            WHEN 'NL' THEN 'Atlantic — Newfoundland'
            WHEN 'QC' THEN 'Quebec'
            WHEN 'ON' THEN 'Ontario'
            WHEN 'MB' THEN 'Prairies'
            WHEN 'SK' THEN 'Prairies'
            WHEN 'AB' THEN 'Alberta'
            WHEN 'BC' THEN 'British Columbia'
            ELSE 'Other'
        END AS ActualTerritory
    FROM TestCases
)
SELECT
    t.ProvinceCode,
    t.ExpectedTerritory,
    a.ActualTerritory,
    CASE WHEN t.ExpectedTerritory = a.ActualTerritory THEN 'PASS' ELSE 'FAIL' END AS Result
FROM TestCases t
INNER JOIN ActualResults a ON a.ProvinceCode = t.ProvinceCode;
```

Unit tests are written by the ETL developer, run frequently during development, and do not require a loaded target table — they test the logic of individual rules.

### 2.2 Integration Testing

**Integration testing** validates a complete package end-to-end: source → all transformations → destination. It verifies that the components work together correctly and that the package produces the expected output in the target table.

Integration tests are run after each package is built and after any modification. They use the live source data against a test copy of the DW.

**Key integration test checks:**
- Package runs to completion without errors (no red tasks in Control Flow)
- Target table has the expected row count
- A sample of rows in the target have correct values for key columns
- No rows appear in error tables (no lookup failures, no DQ rejections)

### 2.3 System Testing

**System testing** validates the complete ETL solution — all packages running together in the correct sequence, against the full dataset. It is the first test that exercises the master package and the inter-package dependencies.

System testing catches:
- Load order violations (a fact loads before its dimension is populated)
- Parallelism issues (two packages that should be independent but actually share a resource)
- Cumulative performance problems (the full load takes longer than the available window)
- Cascading failures (one package failure propagates incorrectly to downstream packages)

### 2.4 Reconciliation Testing

**Reconciliation testing** verifies that the data in the target matches the data in the source — numerically and structurally. This is the most important ongoing test category and the one that runs in production after every load.

Reconciliation tests are the subject of section 4.

### 2.5 Regression Testing

**Regression testing** verifies that changes to an existing ETL system have not broken previously correct behaviour. It is run whenever a package is modified — a new column added, a transformation rule changed, a join updated.

A regression test suite is a collection of tests that represent "known correct" behaviour at a point in time. Every test that passed before the change must still pass after. New tests are added for new behaviour.

```sql
-- Regression test: verify DimCustomer row count has not decreased
-- (guards against accidentally adding a WHERE clause that filters too aggressively)
DECLARE @ExpectedMinRows INT = 100;  -- Known count from last verified load

SELECT
    CASE WHEN COUNT(*) >= @ExpectedMinRows
         THEN 'PASS — Row count maintained'
         ELSE 'FAIL — Row count below expected minimum (' +
              CAST(COUNT(*) AS VARCHAR) + ' vs ' +
              CAST(@ExpectedMinRows AS VARCHAR) + ')'
    END AS RegressionResult
FROM Dimension.DimCustomer
WHERE CustomerID <> 0;
```

---

## 3. Building a Formal Test Suite

A formal test suite is not an ad-hoc collection of verification queries — it is a structured, versioned, and executed set of tests with recorded results. The `ETL.TestResults` table introduced in Chapter 4 is the foundation.

### 3.1 The Test Results Table

```sql
-- Full definition of the test infrastructure tables
USE CabotTrailOutdoorDW;
GO

CREATE SCHEMA ETL;
GO

-- Test results: one row per test execution
CREATE TABLE ETL.TestResults
(
    TestID              INT             NOT NULL IDENTITY(1,1),
    TestName            NVARCHAR(200)   NOT NULL,
    TestCategory        NVARCHAR(50)    NOT NULL,
    TargetTable         NVARCHAR(100)   NOT NULL,
    ExpectedValue       NVARCHAR(200)   NULL,
    ActualValue         NVARCHAR(200)   NULL,
    Passed              BIT             NOT NULL,
    RunDate             DATETIME2       NOT NULL DEFAULT SYSDATETIME(),
    PackageName         NVARCHAR(200)   NULL,
    LoadDurationSeconds INT             NULL,
    Notes               NVARCHAR(500)   NULL,
    CONSTRAINT PK_ETL_TestResults PRIMARY KEY (TestID)
);
GO

-- Test run summary: one row per complete ETL execution
CREATE TABLE ETL.LoadRuns
(
    RunID               INT             NOT NULL IDENTITY(1,1),
    RunStartTime        DATETIME2       NOT NULL,
    RunEndTime          DATETIME2       NULL,
    RunStatus           NVARCHAR(20)    NOT NULL DEFAULT 'Running',
    TotalTests          INT             NOT NULL DEFAULT 0,
    TestsPassed         INT             NOT NULL DEFAULT 0,
    TestsFailed         INT             NOT NULL DEFAULT 0,
    ErrorSummary        NVARCHAR(MAX)   NULL,
    CONSTRAINT PK_ETL_LoadRuns      PRIMARY KEY (RunID),
    CONSTRAINT CK_ETL_LoadRuns_Status
        CHECK (RunStatus IN ('Running', 'Passed', 'Failed', 'Partial'))
);
GO

-- Load errors: rows rejected during ETL
CREATE TABLE ETL.LoadErrors
(
    ErrorID             INT             NOT NULL IDENTITY(1,1),
    RunID               INT             NULL,
    PackageName         NVARCHAR(200)   NOT NULL,
    ComponentName       NVARCHAR(200)   NOT NULL,
    NaturalKeyValue     NVARCHAR(200)   NULL,
    ErrorCode           INT             NULL,
    ErrorDescription    NVARCHAR(MAX)   NULL,
    LoadDate            DATETIME2       NOT NULL DEFAULT SYSDATETIME(),
    CONSTRAINT PK_ETL_LoadErrors    PRIMARY KEY (ErrorID),
    CONSTRAINT FK_ETL_LoadErrors_Run
        FOREIGN KEY (RunID) REFERENCES ETL.LoadRuns (RunID)
);
GO
```

### 3.2 Test Registration and Execution

Tests are not just ad-hoc queries — they are registered entries that are executed consistently on every run. A stored procedure wraps the test logic and records the result:

```sql
-- Stored procedure: execute and record a row count test
CREATE PROCEDURE ETL.usp_TestRowCount
    @TestName       NVARCHAR(200),
    @TargetTable    NVARCHAR(100),
    @ExpectedCount  INT,
    @PackageName    NVARCHAR(200) = NULL
AS
BEGIN
    DECLARE @ActualCount    INT;
    DECLARE @SQL            NVARCHAR(500);
    DECLARE @Passed         BIT;

    -- Dynamic count against target table
    SET @SQL = N'SELECT @cnt = COUNT(*) FROM ' + @TargetTable;
    EXEC sp_executesql @SQL, N'@cnt INT OUTPUT', @cnt = @ActualCount OUTPUT;

    SET @Passed = CASE WHEN @ActualCount = @ExpectedCount THEN 1 ELSE 0 END;

    INSERT INTO ETL.TestResults
        (TestName, TestCategory, TargetTable, ExpectedValue, ActualValue, Passed, PackageName)
    VALUES
        (@TestName, 'RowCount', @TargetTable,
         CAST(@ExpectedCount AS NVARCHAR),
         CAST(@ActualCount   AS NVARCHAR),
         @Passed,
         @PackageName);

    -- Return pass/fail for SSIS package to evaluate
    RETURN CASE WHEN @Passed = 1 THEN 0 ELSE 1 END;  -- 0 = success, 1 = failure
END;
GO
```

```sql
-- Example: running the test from an Execute SQL Task in SSIS
EXEC ETL.usp_TestRowCount
    @TestName       = 'DimCustomer Row Count',
    @TargetTable    = 'Dimension.DimCustomer',
    @ExpectedCount  = 100,
    @PackageName    = 'Load_DimCustomer';
```

### 3.3 The Complete Test Suite for CabotTrail

A production-ready test suite covers every dimension and fact table with at minimum two tests each:

```sql
-- Execute all reconciliation tests (run from master package after all loads)
USE CabotTrailOutdoorDW;

-- ── Dimension tests ───────────────────────────────────────────────
EXEC ETL.usp_TestRowCount 'DimDeliveryMethod Row Count',
    'Dimension.DimDeliveryMethod', 12, 'Load_DimDeliveryMethod';

EXEC ETL.usp_TestRowCount 'DimEmployee Row Count',
    'Dimension.DimEmployee', 50, 'Load_DimEmployee';

EXEC ETL.usp_TestRowCount 'DimCustomer Row Count',
    'Dimension.DimCustomer', 100, 'Load_DimCustomer';

EXEC ETL.usp_TestRowCount 'DimProduct Row Count',
    'Dimension.DimProduct', 142, 'Load_DimProduct';

EXEC ETL.usp_TestRowCount 'DimProductCategory Row Count',
    'Dimension.DimProductCategory', 13, 'Load_DimProductCategory';

-- ── Fact table tests ──────────────────────────────────────────────

-- FactSales: row count
EXEC ETL.usp_TestRowCount 'FactSales Row Count',
    'Fact.FactSales', 16359, 'Load_FactSales';

-- FactSales: revenue reconciliation (aggregate test)
INSERT INTO ETL.TestResults
    (TestName, TestCategory, TargetTable, ExpectedValue, ActualValue, Passed, PackageName)
SELECT
    'FactSales Revenue Reconciliation',
    'Aggregate',
    'Fact.FactSales',
    CAST(ROUND(src.Revenue, 2) AS NVARCHAR),
    CAST(ROUND(dw.Revenue,  2) AS NVARCHAR),
    CASE WHEN ABS(src.Revenue - dw.Revenue) < 0.01 THEN 1 ELSE 0 END,
    'Load_FactSales'
FROM
    (SELECT SUM(il.LineTotal) AS Revenue
     FROM CabotTrailOutdoor.Sales.InvoiceLines il
     INNER JOIN CabotTrailOutdoor.Sales.Invoices i ON i.InvoiceID = il.InvoiceID
     INNER JOIN CabotTrailOutdoor.Sales.Orders o   ON o.OrderID   = i.OrderID
     WHERE YEAR(o.OrderDate) < 2026) src,
    (SELECT SUM(LineTotal) AS Revenue FROM Fact.FactSales) dw;

-- FactSales: referential integrity (orphan check)
INSERT INTO ETL.TestResults
    (TestName, TestCategory, TargetTable, ExpectedValue, ActualValue, Passed, PackageName)
SELECT
    'FactSales Orphan Check',
    'ReferentialIntegrity',
    'Fact.FactSales',
    '0',
    CAST(OrphanCount AS NVARCHAR),
    CASE WHEN OrphanCount = 0 THEN 1 ELSE 0 END,
    'Load_FactSales'
FROM (
    SELECT COUNT(*) AS OrphanCount
    FROM Fact.FactSales fs
    WHERE NOT EXISTS (SELECT 1 FROM Dimension.DimCustomer c
                      WHERE c.CustomerKey = fs.CustomerKey)
    OR    NOT EXISTS (SELECT 1 FROM Dimension.DimProduct p
                      WHERE p.ProductKey  = fs.ProductKey)
    OR    NOT EXISTS (SELECT 1 FROM Dimension.DimEmployee e
                      WHERE e.EmployeeKey = fs.EmployeeKey)
    OR    NOT EXISTS (SELECT 1 FROM Dimension.DimGeography g
                      WHERE g.GeographyKey = fs.GeographyKey)
    OR    NOT EXISTS (SELECT 1 FROM Dimension.DimDeliveryMethod dm
                      WHERE dm.DeliveryMethodKey = fs.DeliveryMethodKey)
) orph;

-- FactPurchasing: row count and aggregate
EXEC ETL.usp_TestRowCount 'FactPurchasing Row Count',
    'Fact.FactPurchasing', 905, 'Load_FactPurchasing';

-- FactReturns: row count and aggregate
EXEC ETL.usp_TestRowCount 'FactReturns Row Count',
    'Fact.FactReturns', 500, 'Load_FactReturns';

-- FactInventory: row count
EXEC ETL.usp_TestRowCount 'FactInventory Row Count',
    'Fact.FactInventory', 142, 'Load_FactInventory';

-- FactCustomerTransactions: row count
EXEC ETL.usp_TestRowCount 'FactCustomerTransactions Row Count',
    'Fact.FactCustomerTransactions', 9346, 'Load_FactCustomerTransactions';

-- FactSupplierTransactions: row count
EXEC ETL.usp_TestRowCount 'FactSupplierTransactions Row Count',
    'Fact.FactSupplierTransactions', 240, 'Load_FactSupplierTransactions';
```

### 3.4 Test Suite Summary Query

After all tests run, a summary query shows the overall pass/fail status:

```sql
-- Test suite summary: current run
SELECT
    TestCategory,
    COUNT(*)                                            AS TotalTests,
    SUM(CASE WHEN Passed = 1 THEN 1 ELSE 0 END)        AS Passed,
    SUM(CASE WHEN Passed = 0 THEN 1 ELSE 0 END)        AS Failed,
    MAX(RunDate)                                        AS LastRun
FROM    ETL.TestResults
WHERE   CAST(RunDate AS DATE) = CAST(GETDATE() AS DATE)
GROUP BY TestCategory
ORDER BY Failed DESC, TestCategory;
```

```sql
-- Failed tests: detail for investigation
SELECT
    TestName,
    TestCategory,
    TargetTable,
    ExpectedValue,
    ActualValue,
    RunDate,
    PackageName
FROM    ETL.TestResults
WHERE   Passed = 0
AND     CAST(RunDate AS DATE) = CAST(GETDATE() AS DATE)
ORDER BY RunDate DESC;
```

---

## 4. Reconciliation Testing in Depth

Reconciliation testing is the most critical ongoing test category. It runs in production after every load and provides the evidence that the ETL system is functioning correctly.

### 4.1 The Three Layers of Reconciliation

Chapter 4 introduced the three-part reconciliation framework. This section extends it with the diagnostic queries needed when each layer fails.

#### Layer 1: Row Count Reconciliation

**What it checks:** The number of rows in the target matches the number of rows expected from the source.

**Why it fails:** Lookup failures (rows dropped at a no-match output), incorrect WHERE clause filters (rows excluded that should not be), JOIN fan-out (rows multiplied by a Cartesian JOIN).

```sql
-- Diagnostic: find the row count difference
WITH SourceCount AS (
    SELECT COUNT(*) AS Rows
    FROM CabotTrailOutdoor.Sales.InvoiceLines il
    INNER JOIN CabotTrailOutdoor.Sales.Invoices i ON i.InvoiceID = il.InvoiceID
    INNER JOIN CabotTrailOutdoor.Sales.Orders o   ON o.OrderID   = i.OrderID
    WHERE YEAR(o.OrderDate) < 2026
),
TargetCount AS (
    SELECT COUNT(*) AS Rows FROM CabotTrailOutdoorDW.Fact.FactSales
),
ErrorCount AS (
    SELECT COUNT(*) AS Rows FROM CabotTrailOutdoorDW.ETL.LoadErrors
    WHERE CAST(LoadDate AS DATE) = CAST(GETDATE() AS DATE)
    AND   PackageName = 'Load_FactSales'
)
SELECT
    s.Rows          AS SourceRows,
    t.Rows          AS TargetRows,
    e.Rows          AS ErrorRows,
    s.Rows - t.Rows AS DiscrepancyRows,
    -- If discrepancy ≈ error count, lookup failure is the cause
    CASE WHEN s.Rows - t.Rows = e.Rows THEN 'Lookup failures explain discrepancy'
         WHEN s.Rows - t.Rows > e.Rows THEN 'Additional rows lost — check JOINs'
         WHEN t.Rows > s.Rows           THEN 'More rows in target than source — check for duplicates'
    END AS Diagnosis
FROM SourceCount s, TargetCount t, ErrorCount e;
```

**Finding fan-out (duplicate rows):** A Cartesian JOIN in the source query multiplies rows. This is a common error when joining to a table without a proper join condition:

```sql
-- Detect fan-out: any InvoiceID appearing more times in DW than in source
SELECT  dw.InvoiceID,
        COUNT(*) AS DW_Count,
        src.Src_Count,
        COUNT(*) - src.Src_Count AS ExtraRows
FROM    CabotTrailOutdoorDW.Fact.FactSales dw
INNER JOIN (
    SELECT InvoiceID, COUNT(*) AS Src_Count
    FROM CabotTrailOutdoor.Sales.InvoiceLines
    GROUP BY InvoiceID
) src ON src.InvoiceID = dw.InvoiceID
GROUP BY dw.InvoiceID, src.Src_Count
HAVING COUNT(*) > src.Src_Count
ORDER BY ExtraRows DESC;
```

#### Layer 2: Aggregate Reconciliation

**What it checks:** Key measures sum to the same value in source and target.

**Why it fails:** Transformation defects (wrong formula for a derived measure), data type precision differences (DECIMAL vs FLOAT in intermediate computations), incorrect filter (some rows included in target but not source aggregate, or vice versa).

```sql
-- Diagnostic: find rows where DW LineTotal differs from source LineTotal
SELECT
    dw.InvoiceID,
    dw.OrderLineID,
    dw.LineTotal        AS DW_LineTotal,
    src.LineTotal       AS Src_LineTotal,
    ABS(dw.LineTotal - src.LineTotal) AS AbsDifference
FROM CabotTrailOutdoorDW.Fact.FactSales dw
INNER JOIN CabotTrailOutdoor.Sales.InvoiceLines src
    ON  src.InvoiceID   = dw.InvoiceID
    AND src.OrderLineID = dw.OrderLineID
WHERE ABS(dw.LineTotal - src.LineTotal) > 0.01
ORDER BY AbsDifference DESC;
```

#### Layer 3: Derived Measure Verification

**What it checks:** Computed measures (`GrossProfit`, `GrossProfitMarginPct`, `UnitCost`) match the documented derivation formula.

This layer verifies that the ETL's transformation logic implements the S2T mapping correctly. The test queries the DW and recomputes the measure independently, comparing stored vs computed values:

```sql
-- Verify GrossProfit derivation: stored value vs recomputed value
SELECT
    fs.SalesKey,
    fs.GrossProfit                                      AS StoredGrossProfit,
    ROUND(fs.LineTotal - fs.UnitCost, 2)                AS ComputedGrossProfit,
    ABS(fs.GrossProfit - ROUND(fs.LineTotal - fs.UnitCost, 2)) AS Discrepancy
FROM CabotTrailOutdoorDW.Fact.FactSales fs
WHERE ABS(fs.GrossProfit - ROUND(fs.LineTotal - fs.UnitCost, 2)) > 0.01
ORDER BY Discrepancy DESC;
-- Expected: 0 rows

-- Verify GrossProfitMarginPct: must match GrossProfit / LineTotal * 100
SELECT
    fs.SalesKey,
    fs.GrossProfitMarginPct                                     AS StoredPct,
    CASE WHEN fs.LineTotal = 0 THEN 0
         ELSE ROUND(fs.GrossProfit / fs.LineTotal * 100, 2)
    END                                                         AS ComputedPct,
    ABS(fs.GrossProfitMarginPct -
        CASE WHEN fs.LineTotal = 0 THEN 0
             ELSE ROUND(fs.GrossProfit / fs.LineTotal * 100, 2)
        END)                                                    AS Discrepancy
FROM CabotTrailOutdoorDW.Fact.FactSales fs
WHERE ABS(fs.GrossProfitMarginPct -
          CASE WHEN fs.LineTotal = 0 THEN 0
               ELSE ROUND(fs.GrossProfit / fs.LineTotal * 100, 2)
          END) > 0.01
ORDER BY Discrepancy DESC;
-- Expected: 0 rows
```

### 4.2 Cross-Database Reconciliation

When data flows through multiple layers (OLTP → DW → data mart), reconciliation must be verified at each boundary:

```sql
-- Cross-database reconciliation: OLTP → DW → datamart
-- All three revenue figures must agree

SELECT 'OLTP Source'       AS Layer, ROUND(SUM(il.LineTotal), 2) AS Revenue
FROM CabotTrailOutdoor.Sales.InvoiceLines il
INNER JOIN CabotTrailOutdoor.Sales.Invoices i ON i.InvoiceID = il.InvoiceID
INNER JOIN CabotTrailOutdoor.Sales.Orders o   ON o.OrderID   = i.OrderID
WHERE YEAR(o.OrderDate) < 2026
UNION ALL
SELECT 'DW Fact.FactSales', ROUND(SUM(LineTotal), 2)
FROM CabotTrailOutdoorDW.Fact.FactSales
UNION ALL
SELECT 'Sales Datamart',    ROUND(SUM(LineTotal), 2)
FROM CabotTrailOutdoorsSales.fact.Sales;
```

All three figures must match. A discrepancy between OLTP and DW indicates a DW ETL defect. A discrepancy between DW and datamart indicates a datamart ETL defect.

---

## 5. Data Quality in Production

Chapter 3 introduced data quality assessment as a design-time activity — profiling source data before ETL is built. This section addresses data quality as a **production concern** — managing and monitoring quality continuously after the ETL system is deployed.

### 5.1 The Data Quality Lifecycle

Data quality is not a one-time assessment. It has a lifecycle that spans the entire operational life of the ETL system:

```
Design time:
  Source profiling → DQ rule definition → Severity assignment → ETL error handling design

Load time:
  DQ rule evaluation → Reject/substitute/continue → Error logging → Notification

Post-load:
  Error table review → Root cause analysis → Source system feedback → Rule adjustment

Ongoing:
  Trend monitoring → Rule maintenance → Periodic source re-profiling
```

The critical point: **data quality degrades over time** if not actively managed. Sources change — new applications are deployed, data entry processes evolve, migrations introduce inconsistencies. A DQ rule that passed 100% on day one may start failing months later as source data patterns change.

### 5.2 Data Quality Trend Monitoring

Recording DQ check results over time enables trend detection:

```sql
-- DQ trend: track NULL rate for key columns over time
-- Run this on a schedule and store results

INSERT INTO ETL.TestResults
    (TestName, TestCategory, TargetTable, ExpectedValue, ActualValue, Passed)
SELECT
    'DimCustomer: NULL SalesTerritory rate',
    'DataQuality',
    'Dimension.DimCustomer',
    '0',
    CAST(SUM(CASE WHEN SalesTerritory IS NULL THEN 1 ELSE 0 END) AS NVARCHAR),
    CASE WHEN SUM(CASE WHEN SalesTerritory IS NULL THEN 1 ELSE 0 END) = 0 THEN 1 ELSE 0 END
FROM Dimension.DimCustomer
WHERE CustomerID <> 0;
```

```sql
-- DQ trend report: how has the NULL rate changed over time?
SELECT
    CAST(RunDate AS DATE)               AS TestDate,
    TestName,
    ActualValue                         AS NullCount,
    Passed
FROM    ETL.TestResults
WHERE   TestName LIKE '%NULL%'
AND     TestCategory = 'DataQuality'
ORDER BY TestName, TestDate;
```

### 5.3 The Unknown Member as a Quality Indicator

In a dimensional model, the unknown member (surrogate key = 0) serves as the default FK for fact rows whose dimension value cannot be resolved. The count of fact rows pointing to the unknown member is a direct measure of data quality:

```sql
-- How many fact rows point to the unknown member for each dimension?
SELECT
    'CustomerKey = 0' AS Dimension,
    COUNT(*) AS UnknownMemberRows,
    ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM Fact.FactSales), 2) AS PctOfTotal
FROM Fact.FactSales WHERE CustomerKey = 0
UNION ALL
SELECT 'ProductKey = 0',
    COUNT(*),
    ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM Fact.FactSales), 2)
FROM Fact.FactSales WHERE ProductKey = 0
UNION ALL
SELECT 'EmployeeKey = 0',
    COUNT(*),
    ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM Fact.FactSales), 2)
FROM Fact.FactSales WHERE EmployeeKey = 0;
```

In a healthy ETL system this query returns 0 for all dimensions. Any non-zero value indicates that lookup failures were handled by routing to the unknown member (acceptable if designed that way) or that dimension records were deleted after fact rows were loaded (a referential integrity concern).

### 5.4 Source Data Change Detection

When source data changes in unexpected ways between loads, the ETL system may produce subtly different results without any errors being raised. Change detection queries compare the current load to the previous load:

```sql
-- Detect unexpected changes in source data between loads
-- Compare today's OLTP row counts to yesterday's DW values

WITH CurrentSourceCounts AS (
    SELECT 'Sales.Customers'    AS TableName, COUNT(*) AS RowCount
    FROM CabotTrailOutdoor.Sales.Customers
    UNION ALL
    SELECT 'Sales.InvoiceLines', COUNT(*)
    FROM CabotTrailOutdoor.Sales.InvoiceLines
    UNION ALL
    SELECT 'Inventory.Products', COUNT(*)
    FROM CabotTrailOutdoor.Inventory.Products WHERE ProductID <> 0
),
PreviousLoadCounts AS (
    -- Most recent passed RowCount test for each table
    SELECT TargetTable AS TableName,
           CAST(ActualValue AS INT) AS RowCount
    FROM (
        SELECT TargetTable, ActualValue,
               ROW_NUMBER() OVER (PARTITION BY TargetTable ORDER BY RunDate DESC) AS rn
        FROM ETL.TestResults
        WHERE TestCategory = 'RowCount' AND Passed = 1
    ) ranked
    WHERE rn = 1
)
SELECT
    c.TableName,
    c.RowCount          AS CurrentRows,
    p.RowCount          AS PreviousRows,
    c.RowCount - ISNULL(p.RowCount, 0) AS RowDelta,
    CASE
        WHEN c.RowCount < ISNULL(p.RowCount, 0)
        THEN '⚠️ Row count decreased — investigate'
        WHEN c.RowCount > ISNULL(p.RowCount, 0) + 1000
        THEN '⚠️ Unusually large increase — verify'
        ELSE '✓ Normal'
    END AS Status
FROM CurrentSourceCounts c
LEFT JOIN PreviousLoadCounts p ON p.TableName = c.TableName;
```

---

## 6. SSIS Logging and Monitoring

An ETL system that runs without monitoring is operating blind. SSIS provides built-in logging infrastructure that, when properly configured, gives complete visibility into every execution.

### 6.1 The SSIS Catalog as a Monitoring Platform

When packages are deployed to the SSIS Catalog (`SSISDB`), every execution is automatically logged. No additional logging configuration is required — the catalog captures:

- Start and end time for every package and every task
- Parameters passed to the execution
- Row counts for every Data Flow component
- All error, warning, and informational messages
- Performance statistics (rows processed, buffer usage)

```sql
-- Recent executions with duration and status
SELECT
    e.execution_id,
    e.package_name,
    e.project_name,
    e.start_time,
    e.end_time,
    DATEDIFF(SECOND, e.start_time, e.end_time)  AS DurationSeconds,
    CASE e.status
        WHEN 1 THEN 'Created'
        WHEN 2 THEN 'Running'
        WHEN 3 THEN 'Cancelled'
        WHEN 4 THEN 'Failed'
        WHEN 5 THEN 'Pending'
        WHEN 6 THEN 'Ended unexpectedly'
        WHEN 7 THEN 'Succeeded'
        WHEN 8 THEN 'Stopping'
        WHEN 9 THEN 'Completed'
    END AS StatusDescription
FROM    SSISDB.catalog.executions e
ORDER BY e.start_time DESC;
```

```sql
-- Error messages from a specific failed execution
SELECT
    om.message_time,
    om.package_name,
    om.task_name,
    om.message
FROM    SSISDB.catalog.operation_messages om
WHERE   om.operation_id = [execution_id]
AND     om.message_type = 120   -- 120 = Error
ORDER BY om.message_time;
```

```sql
-- Row counts per Data Flow component — performance analysis
SELECT
    eds.package_name,
    eds.task_name,
    eds.dataflow_path_id_string  AS Component,
    eds.rows_sent
FROM    SSISDB.catalog.execution_data_statistics eds
WHERE   eds.execution_id = [execution_id]
ORDER BY eds.package_name, eds.task_name;
```

### 6.2 Custom Logging with Execute SQL Tasks

The SSIS Catalog provides operational logs — what happened during execution. Custom logging records business-meaningful metrics — what was loaded, how much, and whether it was correct. Both are needed.

The `ETL.LoadRuns` table introduced in section 3.1 implements custom run-level logging:

```sql
-- Start of master package: create a load run record
-- (Execute SQL Task at the beginning of Master_DW_Load.dtsx)
DECLARE @RunID INT;

INSERT INTO ETL.LoadRuns (RunStartTime, RunStatus)
VALUES (SYSDATETIME(), 'Running');

SET @RunID = SCOPE_IDENTITY();

-- Store RunID in an SSIS package variable for use by child packages
-- (Return value from Execute SQL Task → SSIS variable @RunID)
SELECT @RunID AS RunID;
```

```sql
-- End of master package: update the run record
-- (Execute SQL Task at the end, on both Success and Failure paths)
UPDATE ETL.LoadRuns
SET
    RunEndTime  = SYSDATETIME(),
    RunStatus   = CASE WHEN FailedTests.c = 0 THEN 'Passed' ELSE 'Failed' END,
    TotalTests  = TotalTests.c,
    TestsPassed = PassedTests.c,
    TestsFailed = FailedTests.c
FROM ETL.LoadRuns lr
CROSS JOIN (SELECT COUNT(*) AS c FROM ETL.TestResults
            WHERE CAST(RunDate AS DATE) = CAST(GETDATE() AS DATE)) TotalTests
CROSS JOIN (SELECT COUNT(*) AS c FROM ETL.TestResults
            WHERE CAST(RunDate AS DATE) = CAST(GETDATE() AS DATE) AND Passed = 1) PassedTests
CROSS JOIN (SELECT COUNT(*) AS c FROM ETL.TestResults
            WHERE CAST(RunDate AS DATE) = CAST(GETDATE() AS DATE) AND Passed = 0) FailedTests
WHERE lr.RunID = ?;  -- ? = @RunID SSIS variable
```

### 6.3 Load Performance Monitoring

Tracking load duration over time detects performance degradation before it becomes a production problem:

```sql
-- Load duration trend: is the ETL getting slower over time?
SELECT
    CAST(RunStartTime AS DATE)          AS LoadDate,
    DATEDIFF(SECOND, RunStartTime, RunEndTime) AS DurationSeconds,
    TotalTests,
    TestsPassed,
    TestsFailed,
    RunStatus
FROM    ETL.LoadRuns
WHERE   RunStatus IN ('Passed', 'Failed')
ORDER BY RunStartTime DESC;
```

A sudden increase in load duration typically indicates one of:
- Source data volume grew significantly (expected, but worth confirming)
- A new JOIN or subquery was added without a supporting index
- Blocking or locking from a concurrent process
- A hardware or infrastructure change

### 6.4 Alerting on Failure

In production, ETL failures must trigger immediate notification. SQL Server Agent provides this through **Notifications** — email alerts sent when a job fails. This requires Database Mail to be configured:

```sql
-- Enable Database Mail (run once by DBA)
EXEC sp_configure 'show advanced options', 1;
RECONFIGURE;
EXEC sp_configure 'Database Mail XPs', 1;
RECONFIGURE;

-- Send a test email
EXEC msdb.dbo.sp_send_dbmail
    @profile_name   = 'ETL Alerts',
    @recipients     = 'Patrick.Dolinger@nscc.ca',
    @subject        = 'CabotTrail ETL — Load Failure',
    @body           = 'The nightly ETL load has failed. Please check ETL.LoadRuns for details.';
```

```sql
-- Stored procedure: send alert on ETL failure
CREATE PROCEDURE ETL.usp_SendLoadAlert
    @RunID  INT
AS
BEGIN
    DECLARE @Subject    NVARCHAR(200);
    DECLARE @Body       NVARCHAR(MAX);
    DECLARE @Status     NVARCHAR(20);
    DECLARE @Failed     INT;

    SELECT  @Status = RunStatus,
            @Failed = TestsFailed
    FROM    ETL.LoadRuns
    WHERE   RunID = @RunID;

    IF @Status = 'Failed'
    BEGIN
        SET @Subject = 'CabotTrail ETL FAILED — ' +
                       CAST(@Failed AS NVARCHAR) + ' test(s) failed';
        SET @Body    = 'Load RunID ' + CAST(@RunID AS NVARCHAR) +
                       ' completed with failures. ' + CHAR(13) +
                       'Review ETL.TestResults and ETL.LoadErrors for details.';

        EXEC msdb.dbo.sp_send_dbmail
            @profile_name   = 'ETL Alerts',
            @recipients     = 'Patrick.Dolinger@nscc.ca',
            @subject        = @Subject,
            @body           = @Body;
    END
END;
GO
```

---

## 7. Introduction to Data Governance

Testing and monitoring ensure that the ETL system produces correct data. **Data governance** is the broader organizational framework that ensures data *remains* correct, *is understood* by its users, *is trusted* by decision-makers, and *is managed* responsibly throughout its lifecycle.

### 7.1 What Data Governance Is

Data governance is not a technology — it is a set of **policies, standards, processes, roles, and accountabilities** that define how data is managed within an organization. Technology (including ETL tools, data catalogs, and data quality platforms) supports governance, but governance itself is an organizational capability.

A useful working definition:

> **Data governance** is the exercise of decision-making authority over data assets — defining who can do what with which data, under what conditions, in service of what organizational objectives.

For a BI and ETL context, data governance addresses questions like:
- Who owns each data domain (Sales, Finance, HR) and is accountable for its quality?
- What does "Revenue" mean, precisely, and who approved that definition?
- Who can modify the ETL pipeline, and what approval process governs changes?
- How long is data retained, and when is it archived or deleted?
- Who can access sensitive data (customer PII, financial details), and how is that enforced?

### 7.2 The Four Governance Pillars for BI

In a business intelligence environment, governance clusters around four pillars:

#### Pillar 1: Data Definitions and Standards

Every key business metric must have a single, agreed, documented definition. In the absence of governance, different teams calculate the same metric differently:

- The sales team reports "revenue" as the value on the invoice
- Finance reports "revenue" as the value after returns and adjustments
- Operations reports "revenue" as orders placed, before cancellations

Each team is correct by their own definition. The organization has three different "revenue" numbers and cannot agree on performance. This is the **definition problem** — the most common and most damaging data governance failure.

The ETL developer's role: implement the agreed definition in the S2T mapping and the ETL transformation logic. If no agreed definition exists, **stop and get one before building**. An ETL that implements an unratified definition is encoding a political decision as technical fact.

#### Pillar 2: Data Stewardship

A **data steward** is a business-side person who is accountable for the quality and appropriate use of data within a specific domain. Data stewards:
- Define and maintain business definitions for their domain
- Approve changes to transformation rules that affect their domain
- Review and act on data quality reports
- Arbitrate disputes about data interpretations

In the CabotTrail context:
- The Sales Director is the data steward for customer, product, and revenue data
- The Finance Manager is the data steward for transaction, invoice, and payment data
- The Operations Manager is the data steward for inventory and purchasing data

The ETL developer does not decide what "Customer Category" means — the data steward does. The ETL developer implements the steward's decision.

#### Pillar 3: Data Quality Management

Data quality management is the operational process of monitoring, measuring, and improving data quality continuously. It includes:

- **Quality measurement:** The DQ rules and test suite from this chapter
- **Quality reporting:** Regular reports to data stewards showing quality metrics and trends
- **Issue management:** A process for reporting, tracking, and resolving data quality issues
- **Root cause analysis:** Finding and fixing the underlying source of quality problems, not just the symptoms

Quality problems found in the DW should be traced back to the source system and fixed there — not patched in the ETL transformation. Patching in the ETL creates hidden business rules that the source system continues to violate:

```
Wrong approach: OLTP has inconsistent province codes →
    ETL maps 'N.S.' and 'Nova Scotia' to 'NS' →
    The OLTP continues producing inconsistent codes →
    ETL silently "corrects" them forever

Right approach: OLTP has inconsistent province codes →
    Report to data steward →
    Data steward escalates to application team →
    Application team fixes the data entry validation →
    ETL no longer needs the mapping correction
```

#### Pillar 4: Access Control and Data Security

Not all users should have access to all data. Data governance defines access control policies — who can see what, under what conditions.

For BI systems, access control typically operates at two levels:

**Database level:** SQL Server security — GRANT/DENY/REVOKE on schemas, tables, and views. Analysts get read access to dimension and fact tables; ETL developers get read-write access to their target schema; DBAs have full access.

```sql
-- Example: grant read access to BI analysts on the data mart
CREATE ROLE DataMartReader;

GRANT SELECT ON SCHEMA::fact TO DataMartReader;
GRANT SELECT ON SCHEMA::dim  TO DataMartReader;

-- Add analysts to the role
ALTER ROLE DataMartReader ADD MEMBER [NSCC\john.analyst];
ALTER ROLE DataMartReader ADD MEMBER [NSCC\jane.analyst];
```

**Row and column level:** Some data requires finer-grained access control — for example, hiding salary data from users without HR clearance, or limiting regional managers to their own territory's data. SQL Server supports this through:
- **Row-Level Security (RLS)** — filter rows based on the current user
- **Column-level permissions** — DENY SELECT on specific sensitive columns
- **Dynamic Data Masking** — show masked versions of sensitive values to unauthorized users

### 7.3 The Data Governance Council

In organizations with mature governance programs, a **Data Governance Council** (or Data Stewardship Committee) provides oversight:
- Composed of data stewards from each business domain, IT leadership, and legal/compliance
- Meets regularly to review data quality reports, approve definition changes, and address escalations
- Has authority to allocate resources for data quality remediation

For smaller organizations (and for CabotTrail as a teaching context), governance is lighter — but the principles are the same. Even a single ETL developer and a single business analyst practicing governance (agreeing on definitions, documenting transformations, reviewing DQ reports) is more valuable than a larger team that builds without it.

---

## 8. Metadata Management

**Metadata** is data about data. In an ETL context, metadata describes the structure, lineage, meaning, and quality of the data in the warehouse. Managing metadata is the operational expression of data governance.

### 8.1 Types of Metadata

| Type | Description | Examples |
|---|---|---|
| **Technical metadata** | Physical structure of data | Table names, column names, data types, indexes, constraints |
| **Business metadata** | Business meaning of data | Column descriptions, metric definitions, business rules |
| **Operational metadata** | ETL execution history | Load run times, row counts, error counts, last successful load |
| **Lineage metadata** | Where data came from | Source table, transformation applied, load date |
| **Quality metadata** | Quality measurements | NULL rates, DQ rule pass/fail history, anomaly counts |

### 8.2 Technical Metadata: The System Catalog

SQL Server's system catalog provides complete technical metadata programmatically. The data dictionary query introduced in Chapter 5 of the lab book generates technical metadata from the catalog:

```sql
-- Technical metadata: complete column inventory for the DW
SELECT
    t.TABLE_SCHEMA                  AS [Schema],
    t.TABLE_NAME                    AS [Table],
    c.COLUMN_NAME                   AS [Column],
    c.DATA_TYPE +
    CASE
        WHEN c.CHARACTER_MAXIMUM_LENGTH IS NOT NULL
        THEN '(' + CAST(c.CHARACTER_MAXIMUM_LENGTH AS VARCHAR) + ')'
        WHEN c.NUMERIC_PRECISION IS NOT NULL
         AND c.DATA_TYPE IN ('decimal', 'numeric')
        THEN '(' + CAST(c.NUMERIC_PRECISION AS VARCHAR) + ','
                 + CAST(c.NUMERIC_SCALE AS VARCHAR) + ')'
        ELSE ''
    END                             AS DataType,
    c.IS_NULLABLE                   AS Nullable,
    c.COLUMN_DEFAULT                AS DefaultValue,
    CASE WHEN pk.COLUMN_NAME IS NOT NULL THEN 'PK' ELSE '' END AS KeyType,
    CASE WHEN fk.COLUMN_NAME IS NOT NULL THEN 'FK' ELSE '' END AS FKIndicator
FROM    INFORMATION_SCHEMA.TABLES t
INNER JOIN INFORMATION_SCHEMA.COLUMNS c
    ON c.TABLE_SCHEMA = t.TABLE_SCHEMA AND c.TABLE_NAME = t.TABLE_NAME
LEFT JOIN (
    SELECT ku.TABLE_SCHEMA, ku.TABLE_NAME, ku.COLUMN_NAME
    FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE ku
    INNER JOIN INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
        ON tc.CONSTRAINT_NAME = ku.CONSTRAINT_NAME
        AND tc.CONSTRAINT_TYPE = 'PRIMARY KEY'
) pk ON pk.TABLE_SCHEMA = t.TABLE_SCHEMA
     AND pk.TABLE_NAME  = t.TABLE_NAME
     AND pk.COLUMN_NAME = c.COLUMN_NAME
LEFT JOIN (
    SELECT ku.TABLE_SCHEMA, ku.TABLE_NAME, ku.COLUMN_NAME
    FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE ku
    INNER JOIN INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
        ON tc.CONSTRAINT_NAME = ku.CONSTRAINT_NAME
        AND tc.CONSTRAINT_TYPE = 'FOREIGN KEY'
) fk ON fk.TABLE_SCHEMA = t.TABLE_SCHEMA
     AND fk.TABLE_NAME  = t.TABLE_NAME
     AND fk.COLUMN_NAME = c.COLUMN_NAME
WHERE   t.TABLE_TYPE = 'BASE TABLE'
AND     t.TABLE_SCHEMA IN ('Dimension', 'Fact', 'ETL')
ORDER BY t.TABLE_SCHEMA, t.TABLE_NAME, c.ORDINAL_POSITION;
```

### 8.3 Operational Metadata: The ETL Audit Trail

The `ETL.LoadRuns` and `ETL.TestResults` tables constitute the **operational metadata repository** — a record of every ETL execution with its outcomes.

This operational metadata enables:

**Load history analysis:**
```sql
-- When was each table last successfully loaded?
SELECT
    TargetTable,
    MAX(RunDate)    AS LastSuccessfulLoad,
    COUNT(*)        AS TotalRuns,
    SUM(CASE WHEN Passed = 1 THEN 1 ELSE 0 END) AS PassedRuns
FROM    ETL.TestResults
WHERE   TestCategory = 'RowCount'
AND     Passed = 1
GROUP BY TargetTable
ORDER BY LastSuccessfulLoad;
```

**Freshness monitoring:**
```sql
-- Is the DW data current? Alert if last load was > 25 hours ago
SELECT
    TargetTable,
    MAX(RunDate)                AS LastLoad,
    DATEDIFF(HOUR, MAX(RunDate), GETDATE()) AS HoursSinceLoad,
    CASE WHEN DATEDIFF(HOUR, MAX(RunDate), GETDATE()) > 25
         THEN '⚠️ DATA MAY BE STALE'
         ELSE '✓ Current'
    END                         AS FreshnessStatus
FROM ETL.TestResults
WHERE TestCategory = 'RowCount' AND Passed = 1
GROUP BY TargetTable
ORDER BY HoursSinceLoad DESC;
```

### 8.4 Business Metadata: The Data Dictionary

The S2T mapping from Chapter 3 serves as the business metadata repository for the ETL system. A formal data dictionary extends it with business-side descriptions:

| Column | Technical definition (from catalog) | Business description |
|---|---|---|
| `GrossProfit` | `DECIMAL(18,2) NOT NULL` | Revenue minus cost of goods sold for this invoice line. Cost is computed as UnitPrice × StandardCostPct × PickedQuantity. StandardCostPct is set per product category. Do not sum GrossProfitMarginPct — compute from GrossProfit/LineTotal × 100. |
| `DaysUntilInvoice` | `AS DATEDIFF(DAY, OrderDate, InvoiceDate) PERSISTED` | Number of days elapsed between the customer placing the order and the invoice being issued. Used as a proxy for order fulfillment speed. |
| `SalesTerritory` | `NVARCHAR(60) NULL` | Regional grouping derived from the customer's delivery province code during ETL. Not stored in the OLTP — derived during DW load. |

The business description column is the most valuable — and the one that requires business stakeholder input to write correctly. A data dictionary without business descriptions is incomplete metadata.

---

## 9. Chapter Summary

- **ETL testing** is fundamentally different from application testing: it is set-based, must accommodate changing source data, must catch silent failures, and depends on business-agreed specifications.

- The **five testing levels** are: unit testing (individual transformation logic), integration testing (complete package end-to-end), system testing (all packages together), reconciliation testing (source vs target data agreement), and regression testing (changes have not broken existing behaviour).

- A **formal test suite** records test results in a persistent table (`ETL.TestResults`) after every load. Tests cover row counts, aggregates, referential integrity, and derived measure verification. A summary query provides immediate pass/fail visibility.

- **Reconciliation testing** has three layers: row count (was every row loaded?), aggregate (do key measures sum correctly?), and derived measure verification (are computed columns correct?). Diagnostic queries accompany each layer to isolate defects.

- **Data quality in production** is a continuous activity — monitoring trends, tracking the unknown member rate, detecting source data changes, and feeding quality issues back to source system owners.

- **SSIS logging** through the Catalog provides operational execution history. Custom logging to `ETL.LoadRuns` and `ETL.TestResults` provides business-meaningful audit trails. Alerting via Database Mail ensures failures receive immediate attention.

- **Data governance** is the organizational framework — policies, stewardship, quality management, and access control — that makes data trustworthy over time. The ETL developer's governance responsibilities include implementing agreed definitions, documenting transformations, and not patching source data quality problems silently.

- **Metadata management** covers technical (system catalog), operational (load history), and business (data dictionary) metadata. All three types are necessary for a fully governed BI environment.

---

## 10. Review Questions

1. Explain why ETL reconciliation tests should verify *structural relationships* (e.g., "DW revenue equals source revenue") rather than *specific values* (e.g., "total revenue equals $6,350,582.50"). Under what circumstances would a specific value test be appropriate?

2. The row count reconciliation test for `Fact.FactSales` shows 16,200 rows in the DW versus 16,359 rows expected. The error table shows 159 records logged with `ErrorDescription = 'No match found in lookup Dimension.DimCustomer'`. What does this tell you about the ETL failure, and what two follow-up investigations would you conduct?

3. Describe the difference between a unit test and an integration test in the context of ETL. Give a specific example of each for the `SalesTerritory` derivation in `DimCustomer`.

4. A data steward reports that "revenue" in the BI dashboard does not match the revenue figure in the finance system's monthly close report. Before changing the ETL, what governance process should be followed? Who needs to be involved?

5. The `GrossProfitMarginPct` column has a DQ trend that shows it has been 45.0% for all records for the past three months, then suddenly dropped to an average of 38.5% after a recent load. Describe the investigation you would conduct and what business context might explain this change.

6. Explain the concept of the unknown member in a dimension table. Write a query that reports the unknown member usage rate across all five dimension FKs in `Fact.FactSales`. What would a 5% unknown member rate in `CustomerKey` tell you?

7. A new junior developer has written an ETL package that "corrects" province codes in the OLTP data — mapping `'N.S.'` and `'Nova Scotia'` to `'NS'` within the ETL transformation. Explain why this approach is problematic from a governance perspective, and what the correct approach should be.

8. What is the difference between technical metadata, business metadata, and operational metadata? For the column `DaysToReturn` in `Fact.FactReturns`, write an example of each type.

---

## 🔍 Deeper Dive

### Going Further with Testing, Quality, and Governance

#### Test-Driven ETL Development

**Test-Driven Development (TDD)** is a software development practice where tests are written before code. The developer writes a failing test, writes the minimum code to make it pass, then refactors. Applied to ETL, this is called **Test-Driven ETL Development**.

The process:
1. Write the S2T mapping (the specification)
2. Write the test suite from the mapping (all tests fail — the ETL does not exist yet)
3. Build the ETL until all tests pass
4. Refactor for performance and maintainability (tests continue to pass)

This discipline ensures that the test suite is complete before the ETL is built — not retrofitted afterward, when the temptation is to write tests that match what the ETL actually does rather than what it should do.

For SQL-based ETL, tSQLt is a popular unit testing framework for SQL Server:
[tSQLt — T-SQL Unit Testing Framework](https://tsqlt.org/)

#### Data Observability

**Data observability** is an emerging discipline that extends traditional data quality monitoring with automated anomaly detection. Rather than checking predefined rules ("UnitPrice must be > 0"), data observability tools learn the normal patterns of data and alert when anomalies are detected:

- Unusual changes in row counts (column value distributions shifting)
- Schema changes (a column disappearing or changing type)
- Freshness violations (data not updated within the expected window)
- Volume anomalies (an order-of-magnitude change in row counts)

Commercial data observability platforms include Monte Carlo, Acceldata, and Bigeye. dbt (Data Build Tool) has built-in testing features that implement many data observability concepts:
[dbt Testing Documentation](https://docs.getdbt.com/docs/build/data-tests)

Microsoft's own **Azure Monitor** and **Azure Data Factory monitoring** provide similar capabilities in the Azure ecosystem:
[Monitor Azure Data Factory](https://learn.microsoft.com/en-us/azure/data-factory/monitor-visually)

#### The DAMA-DMBOK Data Quality Framework

The **DAMA-DMBOK** (Data Management Body of Knowledge) provides the most comprehensive academic and practitioner treatment of data quality as an organizational discipline. Its framework defines:

- Six data quality dimensions (accuracy, completeness, consistency, timeliness, uniqueness, validity) — consistent with what this chapter covers
- A data quality management lifecycle (define, measure, analyse, improve, monitor)
- Roles and responsibilities for data quality management
- Integration of data quality with other data management disciplines (governance, metadata, security)

DAMA International. (2017). *DAMA-DMBOK: Data Management Body of Knowledge* (2nd ed.). Technics Publications.

#### Row-Level Security in SQL Server

Row-Level Security (RLS) is a SQL Server feature that transparently filters rows based on the identity of the executing user. For BI systems where different users should see different subsets of data (regional managers see only their region's sales, HR personnel see only their division's records), RLS implements the filter at the database layer rather than in the application or BI tool.

```sql
-- RLS example: restrict dim.Customer rows by SalesTerritory
-- based on the current user

CREATE SCHEMA Security;
GO

-- Security predicate: function that returns 1 for allowed rows
CREATE FUNCTION Security.fn_TerritoryFilter(@SalesTerritory NVARCHAR(60))
RETURNS TABLE
WITH SCHEMABINDING
AS
RETURN
    SELECT 1 AS IsAllowed
    WHERE   @SalesTerritory = (
                SELECT UserTerritory
                FROM   Security.UserTerritoryMapping
                WHERE  UserName = USER_NAME()
            )
    OR USER_NAME() IN ('ETL_Service', 'DBA_Admin');  -- Full access for service accounts
GO

-- Apply the security policy
CREATE SECURITY POLICY Security.TerritoryPolicy
    ADD FILTER PREDICATE Security.fn_TerritoryFilter(SalesTerritory)
    ON dim.Customer
WITH (STATE = ON);
GO
```

Microsoft documentation:
[Row-Level Security — SQL Server](https://learn.microsoft.com/en-us/sql/relational-databases/security/row-level-security)

#### The Data Contract Pattern

An emerging pattern in data engineering is the **data contract** — a formal, versioned agreement between a data producer (source system) and a data consumer (ETL / data warehouse) that specifies:

- The schema of the data being produced
- The quality guarantees (completeness, freshness, constraints)
- The change notification process (how consumers are notified of schema changes)
- The SLA for data availability

Data contracts formalize what this chapter describes informally as the S2T mapping and DQ rule table. They shift the data quality responsibility upstream — the source system agrees to produce data that meets the contract's standards, rather than the ETL developer silently correcting whatever arrives.

Tools implementing data contracts include Soda Core and Great Expectations:
[Soda Core Documentation](https://docs.soda.io/)
[Great Expectations Documentation](https://docs.greatexpectations.io/)

---

### Industry Perspectives

#### Kimball on ETL Testing

Kimball's *The Data Warehouse ETL Toolkit* dedicates a full chapter to ETL testing and quality assurance. His core principle aligns with this chapter:

> *"The ETL system must be tested at every level — unit, integration, system, and acceptance. Testing is not a phase that happens at the end of development; it is an activity that is woven throughout development. An ETL system that has not been thoroughly tested is not ready for production, regardless of how well it was designed."*

Kimball's acceptance testing concept — where business users sign off on data correctness before go-live — is the data governance connection: the business, not just the ETL developer, verifies that the data is correct.

Kimball, R., & Caserta, J. (2004). *The Data Warehouse ETL Toolkit*. Wiley. — Chapter 11 covers testing and quality assurance.

#### Microsoft on SQL Server Security

Microsoft provides comprehensive documentation on all SQL Server security features relevant to data governance:
- [Row-Level Security](https://learn.microsoft.com/en-us/sql/relational-databases/security/row-level-security)
- [Dynamic Data Masking](https://learn.microsoft.com/en-us/sql/relational-databases/security/dynamic-data-masking)
- [SQL Server Audit](https://learn.microsoft.com/en-us/sql/relational-databases/security/auditing/sql-server-audit-database-engine)
- [Always Encrypted](https://learn.microsoft.com/en-us/sql/relational-databases/security/encryption/always-encrypted-database-engine)

---

### References and Further Reading

1. Kimball, R., & Caserta, J. (2004). *The Data Warehouse ETL Toolkit*. Wiley. — Chapter 11 covers ETL testing; Chapter 12 covers quality assurance systems.

2. DAMA International. (2017). *DAMA-DMBOK: Data Management Body of Knowledge* (2nd ed.). Technics Publications. — The authoritative reference for data governance and data quality management disciplines.

3. Redman, T. C. (2008). *Data Driven: Profiting from Your Most Important Business Asset*. Harvard Business Press. — Practitioner-focused treatment of data quality as a business asset.

4. Loshin, D. (2010). *The Practitioner's Guide to Data Quality Improvement*. Morgan Kaufmann. — Covers the data quality lifecycle, measurement frameworks, and improvement processes.

5. Microsoft. (2024). *SSIS Catalog — Monitoring*. [https://learn.microsoft.com/en-us/sql/integration-services/catalog/ssis-catalog](https://learn.microsoft.com/en-us/sql/integration-services/catalog/ssis-catalog)

6. Microsoft. (2024). *Row-Level Security*. [https://learn.microsoft.com/en-us/sql/relational-databases/security/row-level-security](https://learn.microsoft.com/en-us/sql/relational-databases/security/row-level-security)

7. Microsoft. (2024). *Dynamic Data Masking*. [https://learn.microsoft.com/en-us/sql/relational-databases/security/dynamic-data-masking](https://learn.microsoft.com/en-us/sql/relational-databases/security/dynamic-data-masking)

8. tSQLt. (n.d.). *Database Unit Testing for SQL Server*. [https://tsqlt.org/](https://tsqlt.org/)

9. dbt Labs. (2024). *Data Tests — dbt Documentation*. [https://docs.getdbt.com/docs/build/data-tests](https://docs.getdbt.com/docs/build/data-tests)

10. Soda. (2024). *Soda Core Documentation*. [https://docs.soda.io/](https://docs.soda.io/)

11. Eckerson, W. W. (2010). *Performance Dashboards: Measuring, Monitoring, and Managing Your Business* (2nd ed.). Wiley. — Covers the governance and stewardship structures that underpin trusted BI.

---

*Previous chapter: [Chapter 4 — ETL Implementation: Dimensions, Lookups, and Derivations](../chapter-04-etl-implementation/README.md)*

*Next chapter: [Chapter 6 — ETL Documentation: Data Dictionaries and Process Flows](../chapter-06-etl-documentation/README.md)*

---

> **ETL for Business Intelligence** | © Patrick Dolinger, NSCC Institute of Technology
> [CC BY 4.0](https://creativecommons.org/licenses/by/4.0/) — Share and adapt freely with attribution
