# Chapter 7: Advanced ETL — MERGE, Slowly Changing Dimensions, and Multiple Sources

> **ETL for Business Intelligence**
> *A practical guide to data provisioning, dimensional modelling, and pipeline design*
>
> © Patrick Dolinger, NSCC Institute of Technology
> Licensed under [Creative Commons Attribution 4.0 International (CC BY 4.0)](https://creativecommons.org/licenses/by/4.0/)
> You are free to share and adapt this material for any purpose, provided appropriate credit is given.

---

## Chapter Overview

Chapters 3 through 6 built a complete, documented ETL pipeline using the full-reload pattern: truncate the target and reload from scratch on every run. This approach is simple, reliable, and entirely correct for the CabotTrail environment where data volumes are modest and the load window is sufficient.

Most production ETL systems eventually outgrow the full-reload pattern — either because data volumes make full reloads too slow, because source data changes frequently and incremental processing is more efficient, or because the business requires historical dimension tracking that full reloads cannot provide.

This chapter introduces the advanced techniques that address these needs: the T-SQL `MERGE` statement for upsert operations, Slowly Changing Dimension (SCD) implementations in depth, and patterns for integrating data from multiple heterogeneous sources. These are the techniques that distinguish a junior ETL developer from a senior one.

By the end of this chapter you will be able to:

- Write correct, production-quality T-SQL `MERGE` statements for ETL upsert operations
- Explain and implement SCD Types 1, 2, 3, and the Type 6 hybrid
- Choose the appropriate SCD type for a given business requirement
- Design and implement an incremental ETL strategy using watermark timestamps
- Handle data from multiple source systems with conflicting schemas and overlapping keys
- Use the SSIS `MERGE` JOIN and `UNION ALL` transformations for multi-source ETL
- Understand the SSIS SCD Wizard and its limitations

---

## Table of Contents

1. [Beyond Full Reload: When Incremental ETL Is Needed](#1-beyond-full-reload-when-incremental-etl-is-needed)
2. [The T-SQL MERGE Statement](#2-the-t-sql-merge-statement)
3. [MERGE Patterns for ETL](#3-merge-patterns-for-etl)
4. [Slowly Changing Dimensions in Depth](#4-slowly-changing-dimensions-in-depth)
5. [Implementing SCD Type 2](#5-implementing-scd-type-2)
6. [Implementing SCD Type 6](#6-implementing-scd-type-6)
7. [Watermark-Based Incremental Extraction](#7-watermark-based-incremental-extraction)
8. [Multiple Source Integration](#8-multiple-source-integration)
9. [SSIS Patterns for Advanced ETL](#9-ssis-patterns-for-advanced-etl)
10. [Late Arriving Facts and Dimensions](#10-late-arriving-facts-and-dimensions)
11. [Chapter Summary](#11-chapter-summary)
12. [Review Questions](#12-review-questions)
13. [🔍 Deeper Dive](#-deeper-dive)

---

## 1. Beyond Full Reload: When Incremental ETL Is Needed

The full-reload pattern — truncate and reload — has a clean simplicity that makes it the right default for many ETL systems. But it has two fundamental limitations that emerge as systems grow.

### 1.1 The Scalability Limit of Full Reload

Full reload costs are directly proportional to data volume. For CabotTrail with 16,359 fact rows, a full reload completes in seconds. For an enterprise system with 500 million fact rows, a full reload may take 8–12 hours — longer than the available nightly batch window.

The scalability break-even point varies by hardware and query complexity, but a practical rule of thumb: **full reload is appropriate when the load completes within 20% of the available batch window.** This leaves capacity for re-runs on failure and for the window to grow as data accumulates over time.

### 1.2 The History Preservation Limit

Full reload has a more fundamental limitation that has nothing to do with volume: it cannot preserve dimensional history. When a customer moves from Nova Scotia to Ontario, a full reload of `DimCustomer` reflects the new province — and all historical sales rows now appear to be from an Ontario customer, regardless of when they were placed.

SCD Type 2 requires that the ETL detect changes and handle them differently from inserts — which requires incremental processing, not full reload.

### 1.3 Choosing Between Full Reload and Incremental

The choice is not absolute. Many production systems use full reload for small, stable dimensions (lookup tables like `DimDeliveryMethod`) and incremental processing for large, frequently changing tables (transaction facts, SCD Type 2 dimensions).

| Scenario | Recommended approach |
|---|---|
| Small dimension, changes infrequent | Full reload — simplicity outweighs overhead |
| Large fact table, new rows only (append-only) | Incremental insert — filter by `LastModifiedDate > watermark` |
| Dimension with tracked history | Incremental upsert — MERGE with SCD Type 2 logic |
| Large fact table with corrections | Incremental upsert — MERGE on natural key |
| Multiple sources with conflicting records | Staging → MERGE with conflict resolution |

---

## 2. The T-SQL MERGE Statement

The `MERGE` statement (introduced in SQL Server 2008) combines `INSERT`, `UPDATE`, and `DELETE` into a single atomic operation. It compares a **source dataset** to a **target table** row by row, applying different actions depending on whether a match is found.

### 2.1 MERGE Syntax

```sql
MERGE INTO [target_table] AS tgt
USING [source_query_or_table] AS src
ON ([join_condition])

WHEN MATCHED [AND additional_condition] THEN
    [UPDATE SET ... | DELETE]

WHEN NOT MATCHED [BY TARGET] THEN
    INSERT ([columns]) VALUES ([values])

WHEN NOT MATCHED BY SOURCE [AND additional_condition] THEN
    [UPDATE SET ... | DELETE]

[OUTPUT $action, inserted.*, deleted.*];
```

**Key clauses:**

| Clause | Meaning | Typical ETL use |
|---|---|---|
| `WHEN MATCHED` | A source row matches a target row | Update changed attributes |
| `WHEN MATCHED AND (condition)` | Match found AND condition is TRUE | Update only when specific columns changed |
| `WHEN NOT MATCHED BY TARGET` | Source row has no match in target | Insert new row |
| `WHEN NOT MATCHED BY SOURCE` | Target row has no match in source | Handle deleted source records (use with caution in DW) |
| `OUTPUT` | Returns information about each row affected | Audit logging; capturing generated identity values |

### 2.2 A Minimal MERGE Example

Before applying MERGE to dimensional ETL, a simple example illustrates the mechanics:

```sql
-- Source: current data from the OLTP
-- Target: DimEmployee in the DW
-- Action: Update job titles if changed; insert new employees

MERGE INTO CabotTrailOutdoorDW.Dimension.DimEmployee AS tgt
USING
(
    SELECT
        e.EmployeeID,
        p.FullName,
        p.PreferredName,
        e.JobTitle,
        e.Department,
        p.EmailAddress,
        e.HireDate,
        e.IsActive
    FROM CabotTrailOutdoor.Application.Employees e
    INNER JOIN CabotTrailOutdoor.Application.People p
        ON p.PersonID = e.PersonID
    WHERE e.EmployeeID <> 0
) AS src
ON (tgt.EmployeeID = src.EmployeeID)

-- Update only when something has actually changed
WHEN MATCHED AND (
    tgt.FullName    <> src.FullName     OR
    tgt.JobTitle    <> src.JobTitle     OR
    tgt.Department  <> src.Department   OR
    tgt.IsActive    <> src.IsActive
) THEN
    UPDATE SET
        tgt.FullName    = src.FullName,
        tgt.PreferredName = src.PreferredName,
        tgt.JobTitle    = src.JobTitle,
        tgt.Department  = src.Department,
        tgt.EmailAddress = src.EmailAddress,
        tgt.IsActive    = src.IsActive

-- Insert employees that don't exist yet
WHEN NOT MATCHED BY TARGET THEN
    INSERT (EmployeeID, FullName, PreferredName, JobTitle,
            Department, EmailAddress, HireDate, IsActive)
    VALUES (src.EmployeeID, src.FullName, src.PreferredName, src.JobTitle,
            src.Department, src.EmailAddress, src.HireDate, src.IsActive);
```

### 2.3 The Change Detection Condition

The `WHEN MATCHED AND (...)` condition is the most important part of an ETL MERGE. Without it, every matched row is updated on every run — correct data, but:
- Unnecessary writes degrade performance
- `LastModifiedDate` or audit timestamp columns are updated without meaningful changes
- In SCD Type 2 implementations, spurious "changes" trigger new dimension rows

The change detection condition uses OR across all tracked columns — if *any* tracked column has changed, the row is updated:

```sql
-- Change detection: update if ANY tracked column differs
WHEN MATCHED AND (
    tgt.CustomerName    <> src.CustomerName     OR
    tgt.CreditLimit     <> src.CreditLimit      OR
    tgt.ProvinceCode    <> src.ProvinceCode
) THEN UPDATE SET ...
```

**NULL handling in change detection:** Standard comparison operators (`<>`) return NULL (not TRUE or FALSE) when either side is NULL. A column that was NULL and is now populated would not be detected as changed. Use `ISNULL()` or `COALESCE()` to handle NULLable columns:

```sql
-- NULL-safe change detection
WHEN MATCHED AND (
    tgt.CustomerName        <> src.CustomerName                         OR
    tgt.CreditLimit         <> src.CreditLimit                          OR
    -- NULL-safe comparison for nullable column
    ISNULL(tgt.AccountOpenedDate, '1900-01-01')
        <> ISNULL(src.AccountOpenedDate, '1900-01-01')
) THEN UPDATE SET ...
```

### 2.4 The OUTPUT Clause

The `OUTPUT` clause captures information about every row affected by the MERGE, returning it as a result set. In ETL, it is used to:

- Log which rows were inserted, updated, or deleted
- Capture generated surrogate keys (`inserted.CustomerKey`) for use in subsequent operations
- Produce audit records without a separate query

```sql
-- MERGE with OUTPUT for audit logging
DECLARE @AuditLog TABLE (
    Action          NVARCHAR(10),
    EmployeeID      INT,
    OldJobTitle     NVARCHAR(100),
    NewJobTitle     NVARCHAR(100),
    ChangeTime      DATETIME2 DEFAULT SYSDATETIME()
);

MERGE INTO Dimension.DimEmployee AS tgt
USING (...) AS src
ON (tgt.EmployeeID = src.EmployeeID)
WHEN MATCHED AND tgt.JobTitle <> src.JobTitle THEN
    UPDATE SET tgt.JobTitle = src.JobTitle
WHEN NOT MATCHED BY TARGET THEN
    INSERT (EmployeeID, FullName, JobTitle, ...)
    VALUES (src.EmployeeID, src.FullName, src.JobTitle, ...)
OUTPUT
    $action                 AS Action,          -- 'INSERT' or 'UPDATE'
    COALESCE(deleted.EmployeeID, inserted.EmployeeID) AS EmployeeID,
    deleted.JobTitle        AS OldJobTitle,
    inserted.JobTitle       AS NewJobTitle
INTO @AuditLog (Action, EmployeeID, OldJobTitle, NewJobTitle);

-- Review the audit log
SELECT * FROM @AuditLog;
```

---

## 3. MERGE Patterns for ETL

Different ETL scenarios require different MERGE configurations. This section catalogs the most common patterns.

### 3.1 Pattern 1: Upsert (Insert or Update)

The most common ETL MERGE pattern: insert new rows, update changed rows, leave deleted rows untouched.

```sql
-- Pattern: Upsert DimCustomer
-- No WHEN NOT MATCHED BY SOURCE clause — do not delete dimension rows
-- even if source customers are no longer active

MERGE INTO Dimension.DimCustomer AS tgt
USING
(
    SELECT
        c.CustomerID,
        c.CustomerName,
        c.CustomerGroupName         AS CustomerCategoryName,
        c.CreditLimit,
        c.AccountOpenedDate,
        ci.CityName,
        sp.StateProvinceName        AS ProvinceName,
        sp.StateProvinceCode        AS ProvinceCode,
        co.CountryName
    FROM CabotTrailOutdoor.Sales.Customers c
    INNER JOIN CabotTrailOutdoor.Application.Cities ci
        ON ci.CityID = c.DeliveryCityID
    INNER JOIN CabotTrailOutdoor.Application.StateProvinces sp
        ON sp.StateProvinceID = ci.StateProvinceID
    INNER JOIN CabotTrailOutdoor.Application.Countries co
        ON co.CountryID = sp.CountryID
    WHERE c.CustomerID <> 0
) AS src
ON (tgt.CustomerID = src.CustomerID)

WHEN MATCHED AND (
    tgt.CustomerName        <> src.CustomerName         OR
    ISNULL(tgt.CreditLimit, 0) <> ISNULL(src.CreditLimit, 0)
) THEN
    UPDATE SET
        tgt.CustomerName        = src.CustomerName,
        tgt.CustomerCategoryName = src.CustomerCategoryName,
        tgt.CreditLimit         = src.CreditLimit

WHEN NOT MATCHED BY TARGET THEN
    INSERT (CustomerID, CustomerName, CustomerCategoryName,
            CreditLimit, AccountOpenedDate,
            CityName, ProvinceName, ProvinceCode, CountryName)
    VALUES (src.CustomerID, src.CustomerName, src.CustomerCategoryName,
            src.CreditLimit, src.AccountOpenedDate,
            src.CityName, src.ProvinceName, src.ProvinceCode, src.CountryName);
```

### 3.2 Pattern 2: Insert-Only (Append)

For fact tables that only ever grow (new transactions are added; existing transactions are never modified), the MERGE simplifies to an insert-only pattern. A LEFT JOIN detects new rows more efficiently than a full MERGE for large datasets:

```sql
-- Pattern: Insert-only for FactSales (new invoice lines since last load)
-- Assumes watermark-based filtering (covered in section 7)

INSERT INTO Fact.FactSales
(
    OrderDateKey, InvoiceDateKey, DueDateKey,
    CustomerKey, ProductKey, EmployeeKey, GeographyKey, DeliveryMethodKey,
    InvoiceID, OrderID, OrderLineID,
    OrderedQuantity, PickedQuantity, UnitPrice, TaxRate,
    LineTotal, TaxAmount, LineTotalIncludingTax,
    UnitCost, GrossProfit, GrossProfitMarginPct
)
SELECT
    [... extract query ...]
FROM [source tables]
-- Only insert rows not already in the fact table
WHERE NOT EXISTS (
    SELECT 1
    FROM Fact.FactSales existing
    WHERE existing.InvoiceID   = [source].InvoiceID
    AND   existing.OrderLineID = [source].OrderLineID
);
```

### 3.3 Pattern 3: Full Replace for Small Dimensions

For small, stable lookup dimensions (DeliveryMethod, TransactionType), the overhead of MERGE change detection is not worth the added complexity. Full reload (TRUNCATE + INSERT) remains appropriate:

```sql
-- Pattern: Full replace for small static dimensions
TRUNCATE TABLE Dimension.DimDeliveryMethod;

INSERT INTO Dimension.DimDeliveryMethod (DeliveryMethodID, DeliveryMethodName)
SELECT DeliveryMethodID, DeliveryMethodName
FROM CabotTrailOutdoor.Application.DeliveryMethods
WHERE DeliveryMethodID <> 0;
```

**When to use full replace vs MERGE:**
- Full replace: table has < 10,000 rows AND changes are rare AND no downstream dependencies require stable surrogate keys
- MERGE: any table where you need to detect and respond to individual row changes

### 3.4 Pattern 4: Merge with Soft Delete

When source records are logically deleted (marked inactive rather than physically removed), the `WHEN NOT MATCHED BY SOURCE` clause can update the DW dimension to reflect the inactive state:

```sql
-- Pattern: Soft delete — mark inactive in DW when not in active source
MERGE INTO Dimension.DimEmployee AS tgt
USING
(
    -- Source: only active employees
    SELECT e.EmployeeID, p.FullName, e.JobTitle, e.IsActive
    FROM Application.Employees e
    INNER JOIN Application.People p ON p.PersonID = e.PersonID
    WHERE e.EmployeeID <> 0
) AS src
ON (tgt.EmployeeID = src.EmployeeID)

WHEN MATCHED AND tgt.IsActive <> src.IsActive THEN
    UPDATE SET tgt.IsActive = src.IsActive

WHEN NOT MATCHED BY TARGET THEN
    INSERT (EmployeeID, FullName, JobTitle, IsActive)
    VALUES (src.EmployeeID, src.FullName, src.JobTitle, src.IsActive)

-- Employee exists in DW but not in active source: mark inactive
WHEN NOT MATCHED BY SOURCE AND tgt.EmployeeID <> 0 THEN
    UPDATE SET tgt.IsActive = 0;
```

> **Warning on `NOT MATCHED BY SOURCE`:** This clause updates (or deletes) DW rows that have no corresponding source row. In dimensional modelling, physically deleting dimension rows that have historical fact row references would break referential integrity. Use `NOT MATCHED BY SOURCE` only for soft updates (marking `IsActive = 0`) — never for hard deletes on dimensions that have existing fact references.

---

## 4. Slowly Changing Dimensions in Depth

Chapter 2 introduced SCD types conceptually. This chapter implements them. The decision of which SCD type to apply to each dimension attribute is one of the most consequential ETL design decisions — it determines what historical questions the DW can and cannot answer.

### 4.1 The Business Question Test

The correct SCD type for an attribute is determined by the business question it enables. Ask: *"Does a historical report need to show the value at the time of the event, or the value as it is today?"*

**Attribute: Customer Province**
- Historical question: "What was total sales revenue by province in 2022?" — needs the province at the time of each 2022 sale
- Answer requires: SCD Type 2 — preserve historical province values

**Attribute: Customer Credit Limit**
- Historical question: "What was the credit limit for this customer when they placed this order?" — useful for credit analysis
- But: the business may accept "show current credit limit" as sufficient for all reports
- Answer requires: business decision. If historical accuracy matters: Type 2. If current state is sufficient: Type 1.

**Attribute: Employee Job Title**
- Historical question: "Which job title was the sales rep when they made this sale?" — relevant for commission calculations
- Answer requires: SCD Type 2

**Attribute: Product Name**
- Historical question: "Did the product name ever change?"
- Usually: a name correction is a Type 1 fix. A rebrand might warrant Type 2.
- Answer requires: business decision based on frequency and significance of changes.

### 4.2 SCD Type 0: Fixed (Retain Original)

SCD Type 0 is the opposite of Type 1 — the dimension value never changes once loaded. It is appropriate for attributes that represent the state at the time of first occurrence and should never be overwritten:

- `AccountOpenedDate` — the date a customer account was first created; this never changes
- `HireDate` — an employee's original hire date; even if re-hired, the original date is preserved separately

In practice, SCD Type 0 requires no special ETL logic — simply exclude the column from the `WHEN MATCHED THEN UPDATE SET` list and it will never be overwritten.

### 4.3 SCD Type 1: Overwrite

Type 1 is the simplest — update in place, no history preserved. The `WHEN MATCHED AND (change_detected) THEN UPDATE` pattern from section 3.1 implements it.

**CabotTrail dimensions with Type 1 attributes:**
- All dimensions in the current CabotTrail DW are Type 1. This is a deliberate design decision — the business has not yet identified a need for historical dimension tracking. When that need arises, specific attributes can be upgraded to Type 2.

**When Type 1 is acceptable:**
- Corrections (fixing a typo in a customer name — the wrong name was never "correct")
- Attributes whose historical value adds no analytical value
- Attributes that change so rarely that the lack of history is not a practical concern

### 4.4 SCD Type 2: Add New Row (Full History)

Type 2 is the workhorse of dimensional history tracking. Every change to a tracked attribute produces a new dimension row, preserving the old value for historical fact analysis.

**The Type 2 row signature:**

```
CustomerKey | CustomerID | ProvinceCode | ValidFrom   | ValidTo     | IsCurrent
──────────────────────────────────────────────────────────────────────────────
15          | 42         | NS           | 2022-01-01  | 2024-06-30  | 0
87          | 42         | ON           | 2024-07-01  | 9999-12-31  | 1
```

- `CustomerKey = 15` is the surrogate key for the historical NS row — fact rows from 2022–2024 reference this key
- `CustomerKey = 87` is the surrogate key for the current ON row — future fact rows reference this key
- `ValidFrom` / `ValidTo` define the date range during which each row was "current"
- `IsCurrent = 1` identifies the active row for each CustomerID

**Querying Type 2 dimensions:**

```sql
-- Current customer province (IsCurrent filter)
SELECT CustomerID, CustomerName, ProvinceCode
FROM Dimension.DimCustomer
WHERE IsCurrent = 1
AND   CustomerID <> 0
ORDER BY CustomerName;

-- Historical province: what province was this customer in at the time of each sale?
-- (No IsCurrent filter needed — fact rows already reference the correct version)
SELECT
    fs.InvoiceID,
    fs.OrderDate,
    dc.CustomerName,
    dc.ProvinceCode     AS ProvinceAtTimeOfSale,
    fs.LineTotal
FROM Fact.FactSales fs
INNER JOIN Dimension.DimCustomer dc ON dc.CustomerKey = fs.CustomerKey
WHERE dc.CustomerID = 42
ORDER BY fs.OrderDate;
-- Each fact row joins to the dimension version that was current when it was loaded
-- — historical ProvinceCode is preserved correctly
```

### 4.5 SCD Type 3: Add Column (Limited History)

Type 3 adds a `PreviousValue` column to store the immediately prior value alongside the current value:

```
CustomerKey | CustomerID | ProvinceCode | PreviousProvinceCode | ChangedDate
──────────────────────────────────────────────────────────────────────────
15          | 42         | ON           | NS                   | 2024-07-01
```

**Limitations of Type 3:**
- Only one prior value is preserved — a second change overwrites `PreviousProvinceCode`
- Fact rows cannot be correctly attributed to historical values — there is only one dimension row per entity, so joining a fact always returns the current (or previous) value, not the value at the time of the event

**When Type 3 is useful:**
- When only the "before and after the most recent change" is needed
- For slowly changing attributes where a single transition is the maximum expected
- As a lightweight alternative when Type 2's additional rows are not justified

---

## 5. Implementing SCD Type 2

SCD Type 2 requires the most sophisticated ETL logic of all SCD types. The implementation involves detecting changes, expiring old rows, and inserting new ones — all in a single, atomic operation.

### 5.1 The Type 2 Implementation Steps

For each source row arriving in the ETL pipeline:

1. **Look up the current DW row** for this natural key (where `IsCurrent = 1`)
2. **Compare tracked attributes**: has any Type 2 column changed?
3. **If changed:**
   a. Expire the old row: set `ValidTo = today - 1 day` and `IsCurrent = 0`
   b. Insert new row: all current attributes, `ValidFrom = today`, `ValidTo = '9999-12-31'`, `IsCurrent = 1`, new surrogate key generated by IDENTITY
4. **If not changed but row exists:** optionally update Type 1 attributes (non-tracked changes that overwrite in place)
5. **If no current row exists:** insert as a new Type 2 row (first appearance)

### 5.2 T-SQL Implementation: Type 2 for DimCustomer

The cleanest T-SQL approach for Type 2 uses two separate operations: a MERGE that handles Type 1 updates and detects Type 2 changes, followed by an INSERT for the new Type 2 rows.

```sql
-- Step 1: Expire current rows where Type 2 attributes have changed
-- Sets IsCurrent = 0 and ValidTo = today for rows about to be superseded

UPDATE tgt
SET
    tgt.ValidTo     = CAST(GETDATE() AS DATE),
    tgt.IsCurrent   = 0
FROM Dimension.DimCustomer tgt
INNER JOIN
(
    -- Source: current OLTP customer data
    SELECT
        c.CustomerID,
        sp.StateProvinceCode    AS ProvinceCode,
        ci.CityName,
        sp.StateProvinceName    AS ProvinceName
    FROM CabotTrailOutdoor.Sales.Customers c
    INNER JOIN CabotTrailOutdoor.Application.Cities ci
        ON ci.CityID = c.DeliveryCityID
    INNER JOIN CabotTrailOutdoor.Application.StateProvinces sp
        ON sp.StateProvinceID = ci.StateProvinceID
    WHERE c.CustomerID <> 0
) src ON src.CustomerID = tgt.CustomerID
-- Only target current rows where Type 2 attributes have changed
WHERE   tgt.IsCurrent = 1
AND (
    tgt.ProvinceCode    <> src.ProvinceCode     OR
    tgt.CityName        <> src.CityName
);
-- Returns: number of rows expired
```

```sql
-- Step 2: Insert new current rows for changed and new customers

INSERT INTO Dimension.DimCustomer
(
    CustomerID, CustomerName, CustomerCategoryName,
    CreditLimit, AccountOpenedDate, IsOnCreditHold,
    CityName, ProvinceName, ProvinceCode, CountryName,
    PostalCode, SalesTerritory,
    ValidFrom, ValidTo, IsCurrent
)
SELECT
    src.CustomerID,
    src.CustomerName,
    src.CustomerGroupName,
    src.CreditLimit,
    src.AccountOpenedDate,
    0,
    ci.CityName,
    sp.StateProvinceName,
    sp.StateProvinceCode,
    co.CountryName,
    NULL,
    CASE sp.StateProvinceCode
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
    END,
    CAST(GETDATE() AS DATE),    -- ValidFrom: today
    '9999-12-31',               -- ValidTo: open-ended
    1                           -- IsCurrent: this is the current row
FROM CabotTrailOutdoor.Sales.Customers src
INNER JOIN CabotTrailOutdoor.Application.Cities ci
    ON ci.CityID = src.DeliveryCityID
INNER JOIN CabotTrailOutdoor.Application.StateProvinces sp
    ON sp.StateProvinceID = ci.StateProvinceID
INNER JOIN CabotTrailOutdoor.Application.Countries co
    ON co.CountryID = sp.CountryID
WHERE src.CustomerID <> 0
-- Insert new rows where:
-- (a) No current row exists yet (new customer), OR
-- (b) The current row was just expired in Step 1 (changed customer)
AND NOT EXISTS (
    SELECT 1
    FROM Dimension.DimCustomer existing
    WHERE existing.CustomerID = src.CustomerID
    AND   existing.IsCurrent  = 1
);
```

### 5.3 Fact Table Loading with Type 2 Dimensions

When a dimension uses SCD Type 2, the fact table load must resolve the natural key to the surrogate key that was **current at the time of the event** — not the current surrogate key.

For CabotTrail, sales orders are loaded with their actual order date. The Lookup transformation in SSIS must find the dimension row where:
- `CustomerID` matches, AND
- `ValidFrom <= OrderDate AND ValidTo >= OrderDate`

```sql
-- Surrogate key lookup for Type 2 dimension: find the row current at order date
-- (Used in SSIS Lookup reference query or T-SQL join)
SELECT
    CustomerID,
    CustomerKey,
    ValidFrom,
    ValidTo
FROM Dimension.DimCustomer
WHERE CustomerID <> 0;

-- In the fact ETL join:
INNER JOIN Dimension.DimCustomer dc
    ON dc.CustomerID  = il_src.CustomerID
    AND dc.ValidFrom  <= CAST(o.OrderDate AS DATE)
    AND dc.ValidTo    >= CAST(o.OrderDate AS DATE)
```

> **Important:** The SSIS Lookup transformation does not natively support range-based join conditions (`ValidFrom <= OrderDate AND ValidTo >= OrderDate`). For Type 2 dimensions, the lookup must either be implemented as a T-SQL JOIN in the source query, or using the SSIS Lookup with a filtered reference query that pre-resolves to the correct version.

---

## 6. Implementing SCD Type 6

SCD Type 6 (the hybrid of Types 1, 2, and 3) adds current-value columns to the Type 2 structure. Every row carries both the historical attribute value (Type 2, preserved at the time of insertion) and the current attribute value (Type 1, updated on every load to reflect today's state).

### 6.1 The Type 6 Row Structure

```sql
-- Type 6 DimCustomer: both historical and current province
ALTER TABLE Dimension.DimCustomer
ADD CurrentProvinceCode NCHAR(2)        NULL,
    CurrentSalesTerritory NVARCHAR(60)  NULL;
```

After adding these columns, the dimension rows look like:

```
CustomerKey | CustomerID | ProvinceCode | CurrentProvinceCode | ValidFrom   | ValidTo     | IsCurrent
──────────────────────────────────────────────────────────────────────────────────────────────────
15          | 42         | NS           | ON                  | 2022-01-01  | 2024-06-30  | 0
87          | 42         | ON           | ON                  | 2024-07-01  | 9999-12-31  | 1
```

Both rows now carry `CurrentProvinceCode = 'ON'`. Historical fact rows can join to `CustomerKey = 15` to get `ProvinceCode = 'NS'` (the province at time of sale), or to `CurrentProvinceCode = 'ON'` (the customer's current province) — from the same join.

### 6.2 Updating Type 6 Current-Value Columns

On each ETL run, after inserting new Type 2 rows, update the `CurrentProvinceCode` on ALL rows for each CustomerID (both current and expired rows):

```sql
-- Update CurrentProvinceCode on ALL rows for each CustomerID
-- (both IsCurrent = 1 and IsCurrent = 0)
UPDATE tgt
SET
    tgt.CurrentProvinceCode     = src.CurrentProvince,
    tgt.CurrentSalesTerritory   = src.CurrentTerritory
FROM Dimension.DimCustomer tgt
INNER JOIN
(
    -- Get the current province for each customer from the IsCurrent = 1 row
    SELECT
        CustomerID,
        ProvinceCode        AS CurrentProvince,
        SalesTerritory      AS CurrentTerritory
    FROM Dimension.DimCustomer
    WHERE IsCurrent = 1
    AND   CustomerID <> 0
) src ON src.CustomerID = tgt.CustomerID
WHERE tgt.CustomerID <> 0;
```

After this update, every row in `DimCustomer` — historical and current — reflects the customer's *current* province in `CurrentProvinceCode`, while `ProvinceCode` continues to reflect the province at the time that row was created.

---

## 7. Watermark-Based Incremental Extraction

A **watermark** is a stored value — typically a timestamp or sequence number — that marks the point up to which source data has already been extracted. On each ETL run, only records modified after the watermark are extracted.

### 7.1 The Watermark Pattern

```sql
-- Watermark table: stores the last extraction point per source table
USE CabotTrailOutdoorDW;

CREATE TABLE ETL.Watermarks
(
    WatermarkID         INT             NOT NULL IDENTITY(1,1),
    SourceSchema        NVARCHAR(60)    NOT NULL,
    SourceTable         NVARCHAR(100)   NOT NULL,
    WatermarkColumn     NVARCHAR(100)   NOT NULL,
    LastExtractedValue  DATETIME2       NOT NULL,
    LastUpdated         DATETIME2       NOT NULL DEFAULT SYSDATETIME(),
    CONSTRAINT PK_ETL_Watermarks PRIMARY KEY (WatermarkID),
    CONSTRAINT UQ_ETL_Watermarks UNIQUE (SourceSchema, SourceTable)
);

-- Initialize watermarks for CabotTrail source tables
INSERT INTO ETL.Watermarks (SourceSchema, SourceTable, WatermarkColumn, LastExtractedValue)
VALUES
    ('Sales',       'Orders',       'LastEditedWhen', '2022-01-01 00:00:00'),
    ('Sales',       'Customers',    'ValidTo',        '2022-01-01 00:00:00'),
    ('Inventory',   'Products',     'ValidTo',        '2022-01-01 00:00:00');
```

### 7.2 Incremental Extraction Using a Watermark

```sql
-- Read the current watermark
DECLARE @LastExtract DATETIME2;

SELECT @LastExtract = LastExtractedValue
FROM ETL.Watermarks
WHERE SourceSchema = 'Sales' AND SourceTable = 'Orders';

-- Extract only rows modified since last extract
SELECT
    o.OrderID,
    o.CustomerID,
    o.OrderDate,
    o.LastEditedWhen
FROM Sales.Orders o
WHERE o.LastEditedWhen > @LastExtract
ORDER BY o.LastEditedWhen;

-- After successful extract and load, update the watermark
-- (Use the MAX LastEditedWhen from the extracted rows, not GETDATE(),
-- to avoid missing rows edited during the extract window)
UPDATE ETL.Watermarks
SET
    LastExtractedValue  = (SELECT MAX(LastEditedWhen) FROM Sales.Orders
                           WHERE LastEditedWhen > @LastExtract),
    LastUpdated         = SYSDATETIME()
WHERE SourceSchema = 'Sales' AND SourceTable = 'Orders';
```

### 7.3 Watermark Requirements in the Source

Watermark-based extraction requires that source tables have a column that reliably reflects when a row was last modified. In practice:

| Column type | Examples | Reliability |
|---|---|---|
| **Server-maintained timestamp** | `ROWVERSION`, `TIMESTAMP`, database-maintained `LastModifiedAt` | High — cannot be bypassed by application |
| **Application-maintained timestamp** | `LastEditedWhen` (application writes this) | Medium — depends on application being well-behaved |
| **Sequence number** | Auto-incrementing `ChangeSequence` | High for inserts; does not capture updates |
| **No timestamp** | No modification tracking | Not viable for watermark extraction; use Change Data Capture instead |

The CabotTrail OLTP includes `LastEditedWhen` columns on several tables — a common pattern in applications built for ETL compatibility.

### 7.4 Watermark Pitfalls

**Late-arriving data:** Records with a `LastEditedWhen` earlier than the watermark will be missed if they arrive after the extract ran. This is the core limitation of watermark extraction — it assumes that records are modified *before* they arrive in the extraction window.

**Time zone issues:** If the source system and the ETL server are in different time zones, `GETDATE()` on the source and `GETDATE()` on the ETL server may disagree. Always use UTC (`GETUTCDATE()` or `SYSUTCDATETIME()`) for watermark values.

**Batch window overlap:** If the ETL runs while the source system is also being updated, a record modified at exactly the watermark boundary may be missed. Add a small overlap — subtract 5 minutes from the watermark — to ensure records on the boundary are not missed.

---

## 8. Multiple Source Integration

Most enterprise BI environments have more than one source system. The ETL must integrate data from multiple sources into a coherent, consistent dimensional model.

### 8.1 The Integration Challenges

**Overlapping natural keys.** Source System A has `CustomerID = 42`. Source System B also has `CustomerID = 42`. These are different customers. The DW cannot use either natural key directly — it must create a combined key or use a separate mapping table.

```sql
-- Solution: composite natural key with source system identifier
-- DimCustomer gains a SourceSystem column
ALTER TABLE Dimension.DimCustomer
ADD SourceSystem NVARCHAR(20) NOT NULL DEFAULT 'CabotTrailOLTP';

-- The unique constraint becomes the combination
ALTER TABLE Dimension.DimCustomer
ADD CONSTRAINT UQ_DimCustomer_Source UNIQUE (CustomerID, SourceSystem);

-- Lookup in fact ETL uses both columns
INNER JOIN Dimension.DimCustomer dc
    ON dc.CustomerID   = src.CustomerID
    AND dc.SourceSystem = 'CabotTrailOLTP'
    AND dc.IsCurrent    = 1
```

**Conflicting definitions.** Source A defines "Revenue" as invoice amount including tax. Source B defines it as invoice amount excluding tax. The ETL must normalize both to a single agreed definition before loading.

**Different granularity.** Source A records sales at the invoice-line level. Source B records sales as daily summaries. They cannot both feed the same fact table at line-item grain — one must be transformed to match the other's grain.

**Schema differences.** Source A has `CustomerFirstName` and `CustomerLastName` as separate columns. Source B has `CustomerFullName` as a single column. The DW needs `FullName` — the ETL must concatenate from Source A and use directly from Source B.

### 8.2 The Staging Area as an Integration Layer

For multi-source ETL, the **staging area** (introduced in Chapter 1) becomes essential. Each source extracts to its own staging tables, then a conforming layer integrates the staged data before loading the DW dimensions and facts:

```
Source A → Staging.CustomerA  ┐
                               ├── [Conform + deduplicate] → Dimension.DimCustomer
Source B → Staging.CustomerB  ┘

Source A → Staging.SalesA     ┐
                               ├── [Normalize to line-item grain] → Fact.FactSales
Source B → Staging.SalesDailyB┘
```

```sql
-- Staging tables: raw landed data, no transformation
CREATE TABLE Staging.CustomerA
(
    CustomerID      INT             NOT NULL,
    CustomerName    NVARCHAR(100)   NOT NULL,
    ProvinceCode    NCHAR(2)        NULL,
    LoadDate        DATETIME2       NOT NULL DEFAULT SYSDATETIME()
);

CREATE TABLE Staging.CustomerB
(
    ClientCode      NVARCHAR(20)    NOT NULL,   -- Different key format
    ClientFullName  NVARCHAR(200)   NOT NULL,   -- Different column name
    Region          NVARCHAR(50)    NULL,        -- Different attribute
    LoadDate        DATETIME2       NOT NULL DEFAULT SYSDATETIME()
);
```

### 8.3 Deduplication and Master Data

When the same real-world entity (a customer, a product, a supplier) exists in multiple source systems, the ETL must decide whether they represent the same entity or different ones. This is the **master data problem** — one of the most complex challenges in enterprise data integration.

**Deterministic matching:** Match records that share an exact key value (email address, tax ID, ISIN):

```sql
-- Deterministic deduplication: same email = same customer
SELECT
    COALESCE(a.CustomerID, -1 * ROW_NUMBER() OVER (ORDER BY b.ClientCode))
                                AS MasterCustomerID,
    COALESCE(a.CustomerName, b.ClientFullName) AS CustomerName,
    COALESCE(a.ProvinceCode,
             -- Map Source B region codes to province codes
             CASE b.Region
                 WHEN 'Atlantic' THEN 'NS'
                 WHEN 'Central'  THEN 'ON'
                 ELSE NULL
             END)               AS ProvinceCode,
    CASE WHEN a.CustomerID IS NOT NULL THEN 'SourceA'
         ELSE 'SourceB'
    END                         AS MasterSource
FROM Staging.CustomerA a
FULL OUTER JOIN Staging.CustomerB b
    ON b.ClientCode = CAST(a.CustomerID AS NVARCHAR)  -- Key mapping
ORDER BY MasterCustomerID;
```

**Probabilistic matching:** Match records based on similarity scores across multiple attributes (name similarity, address similarity). This requires more sophisticated tooling — covered in the Deeper Dive.

### 8.4 Conformed Dimensions Across Sources

A **conformed dimension** used by multiple sources must use consistent keys so that cross-source analysis is possible. When `DimCustomer` is loaded from both Source A and Source B, both sources' fact tables must reference the same `CustomerKey` values.

The critical discipline: **load conformed dimensions before any fact tables from any source.** The dimension load integrates all sources into a single coherent dimension set. Fact loads from all sources then reference those shared dimension keys.

---

## 9. SSIS Patterns for Advanced ETL

The T-SQL techniques above can be implemented directly using Execute SQL Tasks in SSIS. But SSIS also provides dedicated Data Flow transformations for some advanced scenarios.

### 9.1 The SSIS SCD Transformation Wizard

SSIS includes a built-in **SCD Wizard** that generates a Data Flow for SCD Type 1, Type 2, and Type 3 handling. While convenient for simple cases, the wizard has significant limitations:

**Wizard approach:**

1. Drag an SCD transformation onto the Data Flow canvas
2. Configure: choose the dimension table, identify the natural key column, classify each attribute as Fixed/Type 1/Type 2/Type 3
3. SSIS generates the downstream transformations and OLE DB Commands to expire old rows and insert new ones

**Limitations of the SCD Wizard:**

- Generates `OLE DB Command` transformations for row-by-row updates — extremely slow for large dimensions (one UPDATE statement per changed row, not a set-based operation)
- Does not support composite natural keys well
- Generated packages are difficult to read and maintain
- Cannot be easily extended for Type 6 or custom SCD logic

**Recommendation:** Use the SCD Wizard for learning and prototyping. In production, implement SCD logic as T-SQL stored procedures called from Execute SQL Tasks — set-based operations that are orders of magnitude faster:

```sql
-- Production approach: SCD in a stored procedure called from Execute SQL Task
CREATE PROCEDURE ETL.usp_LoadDimCustomerSCD2
AS
BEGIN
    BEGIN TRANSACTION;

    -- Step 1: Expire changed rows
    UPDATE tgt
    SET ValidTo = CAST(GETDATE() AS DATE), IsCurrent = 0
    FROM Dimension.DimCustomer tgt
    INNER JOIN [... source query ...] src
        ON src.CustomerID = tgt.CustomerID
    WHERE tgt.IsCurrent = 1
    AND (tgt.ProvinceCode <> src.ProvinceCode OR tgt.CityName <> src.CityName);

    -- Step 2: Insert new rows
    INSERT INTO Dimension.DimCustomer (...)
    SELECT ... FROM [... source query ...]
    WHERE NOT EXISTS (SELECT 1 FROM Dimension.DimCustomer WHERE CustomerID = src.CustomerID AND IsCurrent = 1);

    COMMIT TRANSACTION;
END;
GO
```

### 9.2 The Merge Join Transformation

The SSIS **Merge Join** transformation performs a JOIN between two sorted inputs within the Data Flow. Unlike the Lookup transformation (which caches a reference table and performs key resolution), Merge Join performs a full relational join between two data streams.

**Use cases:**
- Joining two large source datasets that are too big to cache in a Lookup
- Detecting changes between two sorted datasets (the "New/Unchanged/Changed/Deleted" change detection pattern)
- Combining rows from two similarly structured sources

**Requirements:**
- Both inputs must be sorted on the join key
- Sorting within the Data Flow uses the Sort transformation (which breaks the streaming architecture and buffers all rows in memory) — expensive
- Prefer pre-sorting in the OLE DB Source SQL query (`ORDER BY`) for best performance

```
Source A (sorted by CustomerID)  ──┐
                                   ├── [Merge Join: Full Outer JOIN on CustomerID]
Source B (sorted by CustomerID)  ──┘
    |
    v
[Conditional Split: detect New/Changed/Deleted]
    ├── New (in A only)      → Insert to target
    ├── Changed (in both)    → Update target
    └── Deleted (in B only)  → Soft delete target
```

### 9.3 The Union All Transformation

The **Union All** transformation combines rows from multiple inputs into a single output stream. It is the SSIS equivalent of the SQL `UNION ALL` operator — no deduplication, no sorting, simply merge all rows.

**Use cases:**
- Loading a dimension from multiple source tables that have the same structure
- Combining similar data from Source A and Source B after schema normalization

```
[OLE DB Source: Customers from System A] ──┐
                                           ├── [Union All] → [OLE DB Destination: DimCustomer]
[OLE DB Source: Clients from System B]  ──┘
```

After the Union All, a Derived Column transformation can add `SourceSystem = 'A'` or `SourceSystem = 'B'` to distinguish rows by origin.

---

## 10. Late Arriving Facts and Dimensions

In a full-reload ETL, late arriving facts are not a concern — every load re-processes all source data. In incremental ETL, they are a real design challenge.

### 10.1 Late Arriving Facts

A **late arriving fact** is a transaction that is loaded into the DW after the period it belongs to has already been processed. Examples:

- An invoice created in December 2024 is entered into the system in January 2025 (data entry backlog)
- A correction to a 2023 order is made in 2025 (retroactive adjustment)
- Data from a partner system arrives 48 hours late (batch timing)

**Detection:**

```sql
-- Find rows where the fact's date is earlier than the last watermark
-- (These are late arrivals that the incremental ETL may have missed)
SELECT
    fs.InvoiceID,
    fs.OrderDate,
    CAST(fs.OrderDate AS DATE) AS FactDate,
    w.LastExtractedValue       AS LastWatermark
FROM CabotTrailOutdoor.Sales.Invoices fs_src
CROSS JOIN (
    SELECT LastExtractedValue
    FROM ETL.Watermarks
    WHERE SourceSchema = 'Sales' AND SourceTable = 'Orders'
) w
WHERE CAST(fs_src.InvoiceDate AS DATE) < CAST(w.LastExtractedValue AS DATE)
AND   fs_src.InvoiceID NOT IN (SELECT InvoiceID FROM Fact.FactSales);
```

**Handling strategies:**

| Strategy | Approach | Trade-off |
|---|---|---|
| **Re-process full period** | Re-run ETL for the affected period (e.g., full December reload) | Accurate but expensive; only practical for recent periods |
| **Insert and flag** | Insert the late fact with correct date keys; add `IsLateArrival` flag | Simple; downstream reports must filter if needed |
| **Hold in staging** | Queue late facts until the next scheduled full re-run | Clean; adds latency |
| **Accept the gap** | Document that late arrivals beyond N days will not be captured | Practical for infrequent late arrivals; requires business agreement |

### 10.2 Late Arriving Dimensions

A **late arriving dimension** is more serious: a fact row references a dimension entity (a customer, a product) that does not yet exist in the DW dimension table.

**Example:** A sales order is loaded before the new customer who placed it has been loaded into `DimCustomer`. The Lookup transformation fails with a no-match.

**Handling strategies:**

**Use the unknown member:** Route the fact row's CustomerKey to the unknown member (CustomerKey = 0). The fact is loaded; the customer is unknown. When the customer loads later, the historical fact rows cannot be retroactively corrected (they reference CustomerKey = 0, not the actual customer's key).

**Hold the fact in staging:** Do not load the fact until its dimension is available. On each run, re-attempt loading from the staging table for any rows previously rejected as late-arriving.

**Create a placeholder dimension row:** Insert a minimal dimension row with known values and a flag `IsPlaceholder = 1`. When the real dimension row arrives, update the placeholder with correct values and clear the flag. Historical fact rows now reference a complete (corrected) dimension row.

```sql
-- Create placeholder for a late-arriving customer
INSERT INTO Dimension.DimCustomer
    (CustomerID, CustomerName, CustomerCategoryName,
     CreditLimit, ProvinceCode, SalesTerritory,
     ValidFrom, ValidTo, IsCurrent, IsPlaceholder)
VALUES
    (@CustomerID, 'Unknown Customer ' + CAST(@CustomerID AS NVARCHAR),
     'Unknown', 0, 'XX', 'Other',
     CAST(GETDATE() AS DATE), '9999-12-31', 1, 1);

-- Later: update with real values when the customer loads
UPDATE Dimension.DimCustomer
SET
    CustomerName        = @RealName,
    CustomerCategoryName = @RealCategory,
    CreditLimit         = @RealCreditLimit,
    ProvinceCode        = @RealProvinceCode,
    IsPlaceholder       = 0
WHERE CustomerID = @CustomerID AND IsCurrent = 1;
```

---

## 11. Chapter Summary

- The **full-reload pattern** is appropriate for small tables and short load windows. As data volumes grow or historical tracking becomes required, **incremental ETL** with MERGE becomes necessary.

- The **T-SQL MERGE statement** combines INSERT, UPDATE, and DELETE in a single atomic operation. The change detection condition (`WHEN MATCHED AND (...)`) is critical — without it, every matched row is unnecessarily rewritten. NULL-safe comparisons are required for nullable columns.

- **MERGE patterns** include upsert (the most common), insert-only append, full replace for small dimensions, and soft delete with `NOT MATCHED BY SOURCE`. The `NOT MATCHED BY SOURCE` clause should never physically delete dimension rows with historical fact references.

- **SCD types** are chosen based on the business question: does a historical report need the attribute value at the time of the event (Type 2) or the current value (Type 1)?

- **SCD Type 2** requires two steps: expire changed rows (UPDATE `ValidTo` and `IsCurrent = 0`) then insert new rows. Fact table loading must join to the dimension version current at the time of the event.

- **SCD Type 6** adds current-value columns to all rows, enabling both historical (`ProvinceCode`) and current-state (`CurrentProvinceCode`) queries from a single join.

- **Watermark-based incremental extraction** stores the last-extracted timestamp per source table and extracts only rows modified since the watermark. It requires source tables to have reliable modification timestamps.

- **Multiple source integration** requires a staging area, key normalization (composite keys or source system identifiers), definition conforming, and granularity alignment. Conformed dimensions must be loaded before any source's fact tables.

- **Late arriving facts** are handled by re-processing, inserting with flags, or staging. **Late arriving dimensions** require placeholder dimension rows or staging until the dimension is available.

---

## 12. Review Questions

1. A production ETL system loads `Fact.FactSales` nightly using full reload. The table currently has 16 million rows and the load takes 4.5 hours. The available batch window is 6 hours. Explain why this system is approaching a scalability problem and describe the incremental ETL strategy you would implement.

2. Write a MERGE statement that loads `Dimension.DimProductCategory` from `Inventory.ProductCategories`. The merge should insert new categories and update `CategoryName` if it has changed. Include appropriate NULL-safe change detection and exclude the unknown member (CategoryID = 0).

3. The `WHEN NOT MATCHED BY SOURCE THEN DELETE` clause of a MERGE would delete dimension rows that no longer exist in the source. Explain specifically why this is dangerous in a dimensional model and what the correct alternative is.

4. A customer moves from Halifax to Toronto. The S2T mapping for `DimCustomer` designates `CityName` and `ProvinceCode` as SCD Type 2 attributes, and `CreditLimit` as SCD Type 1. Describe step by step what happens in the DW when this change is detected during the nightly ETL run. What does the DimCustomer table look like before and after?

5. Explain the difference between `ProvinceCode` and `CurrentProvinceCode` in an SCD Type 6 dimension. Write two queries — one that uses each column — and explain what business question each answers.

6. The source system has a `LastEditedWhen` column on `Sales.Orders`. After implementing watermark-based incremental extraction, you discover that some orders from last week are missing from the DW. Investigation reveals that a batch update process modified these orders without updating `LastEditedWhen`. Identify the root cause and describe two possible solutions.

7. Your ETL is loading facts from both Source A (CabotTrailOutdoor) and a new Source B (a partner retailer's system). Source B uses `PartnerCustomerCode` (a NVARCHAR) as its customer identifier, while Source A uses `CustomerID` (INT). They may or may not represent the same customers. Describe the staging and conforming strategy you would implement to load both into a shared `DimCustomer`.

8. During incremental ETL, a new invoice line arrives for `CustomerID = 999`. Your Lookup transformation against `DimCustomer` finds no matching row for CustomerID 999. Describe the three strategies for handling this late-arriving dimension scenario and explain when each is appropriate.

---

## 🔍 Deeper Dive

### Going Further with Advanced ETL

#### MERGE Performance and Deadlocks

The `MERGE` statement in SQL Server has a documented edge case with deadlocks: when multiple sessions execute MERGE against the same target table simultaneously, they can deadlock. This is because MERGE acquires locks in a non-deterministic order depending on which rows it encounters first.

**Mitigation strategies:**

1. **Serialize MERGE execution:** Ensure only one session executes MERGE against a given target at a time (enforced by the master package's sequential execution model)

2. **Use explicit table hints:** `WITH (HOLDLOCK)` forces MERGE to acquire a range lock upfront, preventing phantom inserts from other sessions:
```sql
MERGE INTO Dimension.DimCustomer WITH (HOLDLOCK) AS tgt
USING (...) AS src
ON (...)
...
```

3. **Use a staging table:** Stage all source data first, then MERGE from the stage to the target in a single session — no concurrency concern

Microsoft Knowledge Base article on MERGE deadlocks:
[MERGE Statement May Cause Deadlocks](https://learn.microsoft.com/en-us/troubleshoot/sql/analysis-services/merge-statement-may-cause-deadlocks)

#### Temporal Tables: Database-Managed SCD Type 2

SQL Server 2016 introduced **system-versioned temporal tables** — a database engine feature that automatically tracks row history without requiring custom SCD logic in the ETL.

A temporal table has a parallel history table maintained by SQL Server:

```sql
-- Create a temporal (system-versioned) table
CREATE TABLE Dimension.DimCustomer_Temporal
(
    CustomerKey     INT             NOT NULL IDENTITY(1,1) PRIMARY KEY,
    CustomerID      INT             NOT NULL,
    CustomerName    NVARCHAR(100)   NOT NULL,
    ProvinceCode    NCHAR(2)        NOT NULL,
    -- System-managed period columns
    ValidFrom       DATETIME2       GENERATED ALWAYS AS ROW START NOT NULL,
    ValidTo         DATETIME2       GENERATED ALWAYS AS ROW END   NOT NULL,
    PERIOD FOR SYSTEM_TIME (ValidFrom, ValidTo)
)
WITH (SYSTEM_VERSIONING = ON (
    HISTORY_TABLE = Dimension.DimCustomer_Temporal_History
));

-- When you UPDATE a row, SQL Server automatically moves the old version
-- to the history table with correct ValidFrom/ValidTo values
UPDATE Dimension.DimCustomer_Temporal
SET ProvinceCode = 'ON'
WHERE CustomerID = 42;

-- Query history: what did this row look like on a given date?
SELECT * FROM Dimension.DimCustomer_Temporal
FOR SYSTEM_TIME AS OF '2023-06-01'
WHERE CustomerID = 42;
```

Temporal tables implement SCD Type 2 semantics at the database engine level — no ETL code required to manage `ValidFrom`/`ValidTo`/`IsCurrent`. The trade-off: temporal tables are designed for operational auditing rather than dimensional modelling, and the history table structure does not match the Kimball SCD Type 2 pattern exactly (no surrogate key per version, no `IsCurrent` flag). They are best suited for OLTP auditing rather than DW dimension tracking.

Microsoft documentation:
[Temporal Tables — SQL Server](https://learn.microsoft.com/en-us/sql/relational-databases/tables/temporal-tables)

#### Probabilistic Matching for Multi-Source Deduplication

When source systems do not share common keys, deterministic matching (same email address = same customer) may not be sufficient. **Probabilistic matching** (also called **fuzzy matching** or **entity resolution**) scores potential matches across multiple attributes and accepts matches above a confidence threshold.

SQL Server Integration Services includes a **Fuzzy Lookup** transformation and a **Fuzzy Grouping** transformation for probabilistic matching within the Data Flow:

```
[Source A Customers]  ──┐
                        ├── [Fuzzy Lookup: match on CustomerName + ProvinceCode]
[Reference: DimCustomer]┘
    |
    v
[Conditional Split: Confidence > 0.85 → matched; else → unmatched]
    ├── Matched → UPDATE existing DimCustomer row
    └── Unmatched → INSERT new DimCustomer row
```

The Fuzzy Lookup uses token-based similarity matching, similar to the Jaro-Winkler or Levenshtein distance algorithms used in natural language processing. It is effective for human names and addresses where exact matches are unreliable.

Microsoft documentation:
[Fuzzy Lookup Transformation — SSIS](https://learn.microsoft.com/en-us/sql/integration-services/data-flow/transformations/fuzzy-lookup-transformation)

#### Change Data Capture as an Alternative to Watermarks

**Change Data Capture (CDC)** reads SQL Server's transaction log to detect changed rows, avoiding the need for `LastEditedWhen` columns in the source schema. It captures INSERT, UPDATE, and DELETE events with the exact before and after values for each changed column.

CDC is more reliable than watermark-based extraction because:
- It captures deletes (watermarks cannot detect deleted rows)
- It detects all changes even when the application does not update a `LastEditedWhen` column
- It provides before-and-after values for each changed column, enabling precise SCD Type 2 detection

```sql
-- CDC: get all changes to Sales.Orders since last extraction
DECLARE @from_lsn BINARY(10) = sys.fn_cdc_get_min_lsn('Sales_Orders');
DECLARE @to_lsn   BINARY(10) = sys.fn_cdc_get_max_lsn();

SELECT
    __$operation,   -- 1=Delete, 2=Insert, 3=Before Update, 4=After Update
    OrderID,
    CustomerID,
    OrderDate,
    __$start_lsn    -- Log sequence number for ordering
FROM cdc.fn_cdc_get_all_changes_Sales_Orders(
    @from_lsn, @to_lsn, 'all with merge'
)
ORDER BY __$start_lsn;
```

CDC requires setup by a DBA and adds some overhead to the source SQL Server (log reading). It is the gold standard for incremental ETL when source schemas do not provide reliable modification timestamps.

Microsoft documentation:
[About Change Data Capture — SQL Server](https://learn.microsoft.com/en-us/sql/relational-databases/track-changes/about-change-data-capture-sql-server)

#### The Data Vault Approach to Multi-Source Integration

The **Data Vault 2.0** methodology (discussed in earlier chapters) was specifically designed for multi-source integration scenarios. Its three table types directly address the challenges of section 8:

- **Hubs** — store the unique natural keys from each source system, one row per business entity
- **Links** — store relationships between hub entities, including cross-source relationships
- **Satellites** — store descriptive attributes from each source separately, with full history

The Hub pattern elegantly handles overlapping natural keys:

```sql
-- Hub: one row per unique customer, regardless of source
CREATE TABLE DV.HubCustomer
(
    CustomerHashKey     BINARY(16)      NOT NULL PRIMARY KEY,  -- MD5 hash of source key + source
    LoadDate            DATETIME2       NOT NULL,
    RecordSource        NVARCHAR(100)   NOT NULL,
    CustomerID          NVARCHAR(50)    NOT NULL,              -- Natural key from source
    SourceSystem        NVARCHAR(20)    NOT NULL
);

-- Satellite: history of customer attributes from Source A
CREATE TABLE DV.SatCustomerA
(
    CustomerHashKey     BINARY(16)      NOT NULL,
    LoadDate            DATETIME2       NOT NULL,
    LoadEndDate         DATETIME2       NULL,
    HashDiff            BINARY(16)      NOT NULL,  -- Hash of all attribute values (change detection)
    CustomerName        NVARCHAR(100),
    ProvinceCode        NCHAR(2),
    PRIMARY KEY (CustomerHashKey, LoadDate)
);
```

Data Vault handles SCD automatically — every change produces a new Satellite row with a new `LoadDate`. The SCD logic is in the pattern, not in custom ETL code.

Linstedt, D., & Olschimke, M. (2015). *Building a Scalable Data Warehouse with Data Vault 2.0*. Morgan Kaufmann.

---

### Industry Perspectives

#### Kimball on Incremental ETL

Kimball's position on incremental ETL is practical: use full reload wherever it works, and introduce incremental processing only where full reload genuinely fails. From *The Data Warehouse ETL Toolkit*:

> *"Incremental ETL is more complex, more fragile, and harder to maintain than full reload. Every incremental ETL system trades simplicity for efficiency. Make that trade only when the efficiency gain is necessary — not as a matter of principle or architectural preference."*

This is important corrective guidance against the tendency to over-engineer ETL with incremental processing before it is needed.

Kimball, R., & Caserta, J. (2004). *The Data Warehouse ETL Toolkit*. Wiley. — Chapters 5–6 cover incremental extraction strategies and SCD implementation patterns in detail.

---

### References and Further Reading

1. Kimball, R., & Caserta, J. (2004). *The Data Warehouse ETL Toolkit*. Wiley. — Chapters 5–6 cover SCD patterns and incremental ETL; Chapter 7 covers multi-source integration.

2. Kimball, R., & Ross, M. (2013). *The Data Warehouse Toolkit* (3rd ed.). Wiley. — Chapters 5–6 cover SCD Types 1–7 in detail with design guidance.

3. Kimball Group. (n.d.). *Slowly Changing Dimension Techniques*. [https://www.kimballgroup.com/data-warehouse-business-intelligence-resources/kimball-techniques/dimensional-modeling-techniques/050-slowly-changing-dimension-types-1-2-3-4-5-6/](https://www.kimballgroup.com/data-warehouse-business-intelligence-resources/kimball-techniques/dimensional-modeling-techniques/050-slowly-changing-dimension-types-1-2-3-4-5-6/)

4. Microsoft. (2024). *MERGE (T-SQL)*. [https://learn.microsoft.com/en-us/sql/t-sql/statements/merge-transact-sql](https://learn.microsoft.com/en-us/sql/t-sql/statements/merge-transact-sql)

5. Microsoft. (2024). *Temporal Tables*. [https://learn.microsoft.com/en-us/sql/relational-databases/tables/temporal-tables](https://learn.microsoft.com/en-us/sql/relational-databases/tables/temporal-tables)

6. Microsoft. (2024). *About Change Data Capture*. [https://learn.microsoft.com/en-us/sql/relational-databases/track-changes/about-change-data-capture-sql-server](https://learn.microsoft.com/en-us/sql/relational-databases/track-changes/about-change-data-capture-sql-server)

7. Microsoft. (2024). *Fuzzy Lookup Transformation — SSIS*. [https://learn.microsoft.com/en-us/sql/integration-services/data-flow/transformations/fuzzy-lookup-transformation](https://learn.microsoft.com/en-us/sql/integration-services/data-flow/transformations/fuzzy-lookup-transformation)

8. Microsoft. (2024). *MERGE Statement May Cause Deadlocks*. [https://learn.microsoft.com/en-us/troubleshoot/sql/analysis-services/merge-statement-may-cause-deadlocks](https://learn.microsoft.com/en-us/troubleshoot/sql/analysis-services/merge-statement-may-cause-deadlocks)

9. Linstedt, D., & Olschimke, M. (2015). *Building a Scalable Data Warehouse with Data Vault 2.0*. Morgan Kaufmann. — Chapters 4–6 cover Hub, Link, and Satellite patterns for multi-source integration.

10. Olson, J. E. (2003). *Data Quality: The Accuracy Dimension*. Morgan Kaufmann. — Covers fuzzy matching and entity resolution techniques for deduplication.

11. Rozenshtein, D. (2012). *The Art of SQL*. O'Reilly Media. — Advanced T-SQL patterns including complex MERGE scenarios and performance optimization.

---

*Previous chapter: [Chapter 6 — ETL Documentation: Data Dictionaries and Process Flows](../chapter-06-etl-documentation/README.md)*

*Next chapter: [Chapter 8 — ETL Administration: Scheduling, Hierarchies, and Migrations](../chapter-08-etl-administration/README.md)*

---

> **ETL for Business Intelligence** | © Patrick Dolinger, NSCC Institute of Technology
> [CC BY 4.0](https://creativecommons.org/licenses/by/4.0/) — Share and adapt freely with attribution
