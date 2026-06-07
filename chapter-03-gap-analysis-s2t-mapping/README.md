# Chapter 3: Gap Analysis and Source-to-Target Mapping

> **ETL for Business Intelligence**
> *A practical guide to data provisioning, dimensional modelling, and pipeline design*
>
> © Patrick Dolinger, NSCC Institute of Technology
> Licensed under [Creative Commons Attribution 4.0 International (CC BY 4.0)](https://creativecommons.org/licenses/by/4.0/)
> You are free to share and adapt this material for any purpose, provided appropriate credit is given.

---

## Chapter Overview

Chapters 1 and 2 established *what* ETL does and *what structure* the data warehouse uses. This chapter addresses the critical design work that must happen before a single ETL component is built: understanding the gap between what the source has and what the target needs, and documenting precisely how to bridge it.

**Gap analysis** and **source-to-target (S2T) mapping** are the ETL developer's design documents. They are the equivalent of an architect's blueprints — the detailed specifications that govern every technical decision that follows. An ETL project built without them will either fail or produce a system that cannot be maintained, tested, or modified with confidence.

This chapter is deliberately practical. It covers the formal methodology and the SQL queries that make that methodology executable in the CabotTrail environment.

By the end of this chapter you will be able to:

- Define the three types of ETL gaps and identify examples of each
- Conduct a systematic source data analysis and profile a source table
- Build a complete source-to-target mapping document for a dimension or fact table
- Identify and document data quality issues using a formal framework
- Apply referential integrity checks to validate source data before ETL begins
- Understand the relationship between the S2T mapping and the SSIS packages that implement it

---

## Table of Contents

1. [Why Design Before You Build](#1-why-design-before-you-build)
2. [The Three Types of ETL Gaps](#2-the-three-types-of-etl-gaps)
3. [Source Data Analysis](#3-source-data-analysis)
4. [Data Quality Assessment](#4-data-quality-assessment)
5. [Source-to-Target Mapping](#5-source-to-target-mapping)
6. [Worked Example: Mapping DimCustomer](#6-worked-example-mapping-dimcustomer)
7. [Worked Example: Mapping FactSales](#7-worked-example-mapping-factsales)
8. [The Relationship Between Mapping and Implementation](#8-the-relationship-between-mapping-and-implementation)
9. [Chapter Summary](#9-chapter-summary)
10. [Review Questions](#10-review-questions)
11. [🔍 Deeper Dive](#-deeper-dive)

---

## 1. Why Design Before You Build

A common instinct among developers encountering a new ETL project is to start building immediately — connect to the source, pull data, write transformations, load the target. This approach produces working code quickly and feels productive. It is also one of the most reliable paths to an ETL system that cannot be trusted.

The reason is straightforward: ETL transforms data according to business rules. Those rules must be agreed upon, documented, and validated before they are encoded in a transformation pipeline. If the rules are encoded first and documented afterward (or never), several things happen:

**The rules become invisible.** Three months after a package is built, no one can answer the question "why does `SalesTerritory` have the value 'Atlantic — Nova Scotia' for this customer?" The answer is buried in a SSIS Derived Column expression that took twelve minutes to find.

**Testing becomes impossible.** A test suite verifies that the system produces correct output. Correct output requires a specification. A specification requires documentation. Without the S2T mapping, there is no specification to test against.

**Changes become dangerous.** When a business rule changes — and it will — the developer needs to find every place that rule is implemented and update all of them consistently. Without documentation, finding those places requires archaeology.

**Onboarding becomes impossible.** A new developer joining the project has no way to understand what the system is supposed to do without reading every package, every SQL statement, and every expression — and hoping they are all consistent.

The S2T mapping is the solution to all of these problems. It is a living document that precedes implementation, guides implementation, and is maintained alongside implementation throughout the life of the ETL system.

---

## 2. The Three Types of ETL Gaps

A **gap** is any difference between what the source provides and what the target requires. Every ETL project has gaps. The work of gap analysis is to find them all, categorize them, and document how each will be resolved.

There are three fundamental gap types.

### 2.1 Missing Data Gaps

A **missing data gap** occurs when the target requires a column or attribute that does not exist in any source system.

Missing data gaps are the most significant because they require either sourcing the data from a new place, deriving it from existing data, or accepting that the target column will always be NULL or a default value.

**CabotTrail examples:**

| Target Column | Target Table | Gap Description | Resolution |
|---|---|---|---|
| `SalesTerritory` | `dim.Customer` (datamart) | No `SalesTerritory` column exists in any OLTP table | Derive from `ProvinceCode` using a CASE expression during ETL |
| `PostalCode` | `dim.Customer` (datamart) | Postal code not captured in `Application.Cities` | NULL default — not available in source; document as gap |
| `ProductDescription` | `dim.Product` (datamart) | `Inventory.Products` has no description column | NULL default |
| `IsOnCreditHold` | `dim.Customer` (datamart) | `Sales.Customers.IsOnCreditHold` was not loaded into DW | Default to 0 — not available via DW; source directly from OLTP if needed |

Missing data gaps require a design decision. The options are:

1. **Derive it** — compute the value from existing data (e.g., `SalesTerritory` from `ProvinceCode`)
2. **Source it elsewhere** — find the value in another system or table
3. **Default it** — use a fixed default value and document why
4. **Accept NULL** — document that the column is intentionally unpopulated

The decision must be made by a business stakeholder, not unilaterally by the ETL developer. "The source doesn't have it so I left it NULL" is not an acceptable resolution unless the business has agreed that NULL is correct.

### 2.2 Data Quality Gaps

A **data quality gap** occurs when source data exists but is dirty, inconsistent, incomplete, or violates business rules that the target enforces.

Data quality gaps are often the most numerous and the most time-consuming to resolve. They represent the real-world messiness of operational data — data that was entered by humans under time pressure, migrated from legacy systems, or generated by applications that did not enforce consistent rules.

**Common data quality gap types:**

| Type | Description | Example |
|---|---|---|
| **NULL violations** | A NOT NULL target column receives NULL from source | `CustomerName` is NULL for 3 records |
| **Type mismatches** | Source stores a value as the wrong type | `OrderDate` stored as NVARCHAR('2024-03-15') instead of DATE |
| **Range violations** | A numeric value falls outside the valid range | `UnitPrice = -5.00` in a source with no negative price validation |
| **Referential integrity** | A FK value in the source references a PK that does not exist | `Orders.CustomerID = 999` but `CustomerID = 999` does not exist in `Customers` |
| **Encoding inconsistency** | The same concept expressed differently | Province stored as 'NS', 'N.S.', 'Nova Scotia', and 'nova scotia' |
| **Duplicates** | The same entity appears multiple times with different keys | Two `Customers` rows for 'Fundy Bay Outfitters' with different CustomerIDs |
| **Orphaned records** | Records with no valid parent | Returns with no matching order line |
| **Future dates** | Transaction dates in the future | `InvoiceDate = '2099-01-01'` (a data entry error) |
| **Truncated data** | Values cut off because a source column was too narrow | `CustomerName = 'The Great Outdoors Supply Comp'` (truncated at 30 chars) |

### 2.3 Structural Gaps

A **structural gap** occurs when the source organizes data differently from how the target needs it — requiring joins, splits, pivots, or other structural transformations.

Structural gaps are the most technically interesting because they require the most sophisticated ETL logic.

**CabotTrail examples:**

| Structural gap | Description | Resolution |
|---|---|---|
| Geography normalization | Customer geography is spread across `Customers`, `Cities`, `StateProvinces`, `Countries` — 4 tables | JOIN all four at extract time into a single flat row |
| M:M category relationship | A product can belong to multiple categories (junction table) | Apply business rule: use MIN(ProductCategoryID) as primary category |
| Snowflake to star | DW `DimProduct` snowflakes to `DimProductCategory` and `DimSupplier`; datamart needs a flat wide dimension | JOIN all three in extract query; flatten into single dim.Product row |
| Integer date key | Source stores dates as DATE; DW requires YYYYMMDD integer | Convert using `CAST(FORMAT(OrderDate, 'yyyyMMdd') AS INT)` |
| Surrogate key lookup | Source stores `CustomerID` (natural key); fact requires `CustomerKey` (surrogate) | Lookup transformation in SSIS; JOIN in T-SQL |
| Fiscal year derivation | Source has no fiscal year attributes; DW `DimDate` requires fiscal year, quarter, period | Compute from calendar month using business fiscal calendar rules |

---

## 3. Source Data Analysis

Before gaps can be identified, the source data must be thoroughly understood. **Source data analysis** is the systematic profiling of every source table to establish its structure, content, ranges, completeness, and quality.

Source data analysis answers the questions:
- What is in this table?
- How many rows?
- What is the date range?
- Are there NULLs? How many, in which columns?
- Are there outliers or unexpected values?
- Are referential integrity relationships intact?

### 3.1 Table-Level Profiling

The first step is to understand the shape and size of every source table.

```sql
-- Fast row counts for all OLTP tables (uses metadata — no full scan)
USE CabotTrailOutdoor;

SELECT
    SCHEMA_NAME(t.schema_id)    AS [Schema],
    t.name                      AS [Table],
    p.rows                      AS [RowCount],
    -- Table creation date
    t.create_date               AS CreatedDate
FROM    sys.tables t
INNER JOIN sys.partitions p
    ON p.object_id = t.object_id
    AND p.index_id IN (0, 1)   -- Heap (0) or clustered index (1)
ORDER BY [Schema], t.name;
```

```sql
-- Column inventory: data types, nullability, defaults
SELECT
    c.TABLE_SCHEMA,
    c.TABLE_NAME,
    c.COLUMN_NAME,
    c.DATA_TYPE
        + CASE
            WHEN c.CHARACTER_MAXIMUM_LENGTH IS NOT NULL
            THEN '(' + CAST(c.CHARACTER_MAXIMUM_LENGTH AS VARCHAR) + ')'
            WHEN c.NUMERIC_PRECISION IS NOT NULL
             AND c.DATA_TYPE IN ('decimal', 'numeric')
            THEN '(' + CAST(c.NUMERIC_PRECISION AS VARCHAR)
                + ',' + CAST(c.NUMERIC_SCALE AS VARCHAR) + ')'
            ELSE ''
          END                   AS DataType,
    c.IS_NULLABLE,
    c.COLUMN_DEFAULT,
    c.ORDINAL_POSITION
FROM    INFORMATION_SCHEMA.COLUMNS c
WHERE   c.TABLE_SCHEMA IN ('Sales', 'Purchasing', 'Inventory', 'Application')
ORDER BY c.TABLE_SCHEMA, c.TABLE_NAME, c.ORDINAL_POSITION;
```

### 3.2 Column-Level Profiling

After understanding the structure, profile the content of key columns in each source table. Column profiling establishes value distributions, NULL rates, and data ranges.

```sql
-- Profile: Sales.Orders
USE CabotTrailOutdoor;

SELECT
    COUNT(*)                            AS TotalRows,
    COUNT(DISTINCT CustomerID)          AS UniqueCustomers,
    COUNT(DISTINCT SalesRepID)          AS UniqueSalesReps,
    COUNT(DISTINCT CityID)              AS UniqueCities,
    MIN(OrderDate)                      AS EarliestOrderDate,
    MAX(OrderDate)                      AS LatestOrderDate,
    -- NULL checks
    SUM(CASE WHEN OrderDate  IS NULL THEN 1 ELSE 0 END) AS NullOrderDates,
    SUM(CASE WHEN CustomerID IS NULL THEN 1 ELSE 0 END) AS NullCustomerIDs,
    SUM(CASE WHEN SalesRepID IS NULL THEN 1 ELSE 0 END) AS NullSalesRepIDs,
    SUM(CASE WHEN CityID     IS NULL THEN 1 ELSE 0 END) AS NullCityIDs
FROM    Sales.Orders;
```

```sql
-- Profile: Sales.InvoiceLines — numeric value ranges and quality checks
SELECT
    COUNT(*)                            AS TotalRows,
    MIN(UnitPrice)                      AS MinUnitPrice,
    MAX(UnitPrice)                      AS MaxUnitPrice,
    ROUND(AVG(UnitPrice), 2)            AS AvgUnitPrice,
    MIN(Quantity)                       AS MinQuantity,
    MAX(Quantity)                       AS MaxQuantity,
    MIN(LineTotal)                      AS MinLineTotal,
    MAX(LineTotal)                      AS MaxLineTotal,
    -- Quality flags
    SUM(CASE WHEN UnitPrice <= 0  THEN 1 ELSE 0 END) AS ZeroOrNegativePrice,
    SUM(CASE WHEN Quantity  <= 0  THEN 1 ELSE 0 END) AS ZeroOrNegativeQty,
    SUM(CASE WHEN TaxRate   <  0  THEN 1 ELSE 0 END) AS NegativeTaxRate,
    SUM(CASE WHEN LineTotal <  0  THEN 1 ELSE 0 END) AS NegativeLineTotal
FROM    Sales.InvoiceLines;
```

```sql
-- Profile: Inventory.Products — NULL analysis and value distribution
SELECT
    COUNT(*)                            AS TotalProducts,
    SUM(CASE WHEN ProductCode IS NULL       THEN 1 ELSE 0 END) AS NullProductCodes,
    SUM(CASE WHEN ColorID     IS NULL       THEN 1 ELSE 0 END) AS NullColors,
    SUM(CASE WHEN IsDiscontinued = 1        THEN 1 ELSE 0 END) AS DiscontinuedCount,
    MIN(UnitPrice)                      AS MinUnitPrice,
    MAX(UnitPrice)                      AS MaxUnitPrice,
    ROUND(AVG(UnitPrice), 2)            AS AvgUnitPrice,
    MIN(TypicalWeightPerUnit)           AS MinWeight,
    MAX(TypicalWeightPerUnit)           AS MaxWeight
FROM    Inventory.Products
WHERE   ProductID <> 0;
```

### 3.3 Referential Integrity Checks

Referential integrity (RI) checks verify that FK values in child tables have corresponding PK values in parent tables. In a well-maintained OLTP, these should always pass. But legacy data migrations, application bugs, and data loads that bypass constraints can introduce orphaned records.

RI failures in the source are particularly dangerous for ETL because they cause **lookup failures** — a fact row that references a `CustomerID` that does not exist in the `Customers` table will fail the surrogate key lookup in SSIS, potentially causing the entire load to fail or silently dropping rows.

```sql
-- RI check: Orders with no matching Customer
SELECT  COUNT(*) AS OrphanOrders
FROM    Sales.Orders o
WHERE   NOT EXISTS (
    SELECT 1 FROM Sales.Customers c
    WHERE c.CustomerID = o.CustomerID
);

-- RI check: OrderLines with no matching Order
SELECT  COUNT(*) AS OrphanOrderLines
FROM    Sales.OrderLines ol
WHERE   NOT EXISTS (
    SELECT 1 FROM Sales.Orders o
    WHERE o.OrderID = ol.OrderID
);

-- RI check: InvoiceLines with no matching OrderLine
SELECT  COUNT(*) AS OrphanInvoiceLines
FROM    Sales.InvoiceLines il
WHERE   NOT EXISTS (
    SELECT 1 FROM Sales.OrderLines ol
    WHERE ol.OrderLineID = il.OrderLineID
);

-- RI check: Products with no matching Supplier
SELECT  COUNT(*) AS ProductsWithNoSupplier
FROM    Inventory.Products p
WHERE   NOT EXISTS (
    SELECT 1 FROM Purchasing.Suppliers s
    WHERE s.SupplierID = p.SupplierID
)
AND p.ProductID <> 0;

-- RI check: ProductCategoryAssignments with no matching Product
SELECT  COUNT(*) AS OrphanCategoryAssignments
FROM    Inventory.ProductCategoryAssignments pca
WHERE   NOT EXISTS (
    SELECT 1 FROM Inventory.Products p
    WHERE p.ProductID = pca.ProductID
);
```

> **In the CabotTrail environment, all RI checks should return 0.** If they do not, the source database was not loaded correctly and must be corrected before ETL development begins. Document the result of every RI check in the source data analysis report.

### 3.4 Cardinality Analysis

Understanding the cardinality (number of distinct values) of key columns helps predict ETL behaviour and identify potential design issues.

```sql
-- Cardinality of key columns in Products
SELECT
    COUNT(DISTINCT ProductID)       AS UniqueProducts,
    COUNT(DISTINCT SupplierID)      AS UniqueSuppliers,
    COUNT(DISTINCT ColorID)         AS UniqueColors,
    COUNT(DISTINCT PackageTypeID)   AS UniquePackageTypes
FROM    Inventory.Products
WHERE   ProductID <> 0;

-- How many products per category? (M:M junction analysis)
SELECT
    pc.ProductCategoryName,
    COUNT(pca.ProductID)            AS ProductCount
FROM    Inventory.ProductCategories pc
LEFT JOIN Inventory.ProductCategoryAssignments pca
    ON pca.ProductCategoryID = pc.ProductCategoryID
GROUP BY pc.ProductCategoryName
ORDER BY ProductCount DESC;

-- Products assigned to multiple categories (important for primary category logic)
SELECT
    p.ProductName,
    COUNT(pca.ProductCategoryID)    AS CategoryCount,
    STRING_AGG(pc.ProductCategoryName, ', ')
        WITHIN GROUP (ORDER BY pca.ProductCategoryID) AS Categories
FROM    Inventory.Products p
INNER JOIN Inventory.ProductCategoryAssignments pca
    ON pca.ProductID = p.ProductID
INNER JOIN Inventory.ProductCategories pc
    ON pc.ProductCategoryID = pca.ProductCategoryID
WHERE   p.ProductID <> 0
GROUP BY p.ProductName
HAVING  COUNT(pca.ProductCategoryID) > 1
ORDER BY CategoryCount DESC;
```

The multi-category query above reveals products that belong to more than one category. This is a **structural gap** — the target `DimProduct` needs a single category per product. The source data analysis reveals the business decision that must be made: which category takes precedence? The CabotTrail ETL resolves this by always using the lowest `ProductCategoryID` (the primary assignment).

---

## 4. Data Quality Assessment

Source data analysis produces raw facts about the data. A **data quality assessment** turns those facts into a structured framework of rules, measurements, and severities that guides ETL error handling.

### 4.1 Data Quality Dimensions

Data quality is assessed across several dimensions — each measuring a different aspect of "good" data:

| Dimension | Definition | Example |
|---|---|---|
| **Completeness** | Are all required values present? | `CustomerName IS NOT NULL` |
| **Accuracy** | Do values correctly represent the real world? | `UnitPrice > 0` |
| **Consistency** | Are values consistent within and across systems? | Province stored as 'NS' everywhere (not 'N.S.' or 'Nova Scotia') |
| **Timeliness** | Is the data current enough for its intended use? | Inventory snapshot taken within the last 24 hours |
| **Uniqueness** | Are entities represented exactly once? | No duplicate CustomerID values |
| **Validity** | Do values conform to defined formats or ranges? | `OrderDate` is a valid date, not '0000-00-00' |
| **Referential integrity** | Do FK values have corresponding PK records? | All `Orders.CustomerID` values exist in `Customers.CustomerID` |

### 4.2 The Data Quality Rule Table

Document every data quality check as a formal rule with a defined pass condition, measurement query, and severity level.

**Severity levels:**
- **High** — a failure blocks the load; affected rows must be rejected or the ETL must stop
- **Medium** — a failure is logged and rows are flagged; the load continues
- **Low** — a failure is logged only; no operational impact

```sql
-- Execute and document these DQ rules for the CabotTrail source

-- DQ-001 [High] Sales.Orders: OrderDate must not be NULL
SELECT 'DQ-001' AS RuleID, 'High' AS Severity,
       'Sales.Orders' AS [Table], 'OrderDate' AS [Column],
       'No null order dates' AS RuleDescription,
       COUNT(*) AS FailCount
FROM Sales.Orders WHERE OrderDate IS NULL;

-- DQ-002 [High] Sales.Orders: CustomerID must exist in Customers
SELECT 'DQ-002' AS RuleID, 'High' AS Severity,
       'Sales.Orders' AS [Table], 'CustomerID' AS [Column],
       'CustomerID must reference a valid customer' AS RuleDescription,
       COUNT(*) AS FailCount
FROM Sales.Orders o
WHERE NOT EXISTS (SELECT 1 FROM Sales.Customers c WHERE c.CustomerID = o.CustomerID);

-- DQ-003 [High] Sales.InvoiceLines: UnitPrice must be > 0
SELECT 'DQ-003' AS RuleID, 'High' AS Severity,
       'Sales.InvoiceLines' AS [Table], 'UnitPrice' AS [Column],
       'Unit price must be positive' AS RuleDescription,
       COUNT(*) AS FailCount
FROM Sales.InvoiceLines WHERE UnitPrice <= 0;

-- DQ-004 [High] Sales.InvoiceLines: Quantity must be > 0
SELECT 'DQ-004' AS RuleID, 'High' AS Severity,
       'Sales.InvoiceLines' AS [Table], 'Quantity' AS [Column],
       'Quantity must be positive' AS RuleDescription,
       COUNT(*) AS FailCount
FROM Sales.InvoiceLines WHERE Quantity <= 0;

-- DQ-005 [Medium] Inventory.Products: ProductCode may be NULL
SELECT 'DQ-005' AS RuleID, 'Medium' AS Severity,
       'Inventory.Products' AS [Table], 'ProductCode' AS [Column],
       'ProductCode is nullable; document NULL count' AS RuleDescription,
       COUNT(*) AS FailCount
FROM Inventory.Products WHERE ProductCode IS NULL AND ProductID <> 0;

-- DQ-006 [High] Inventory.Products: UnitPrice must be > 0
SELECT 'DQ-006' AS RuleID, 'High' AS Severity,
       'Inventory.Products' AS [Table], 'UnitPrice' AS [Column],
       'Unit price must be positive' AS RuleDescription,
       COUNT(*) AS FailCount
FROM Inventory.Products WHERE UnitPrice <= 0 AND ProductID <> 0;

-- DQ-007 [High] Sales.Customers: CreditLimit must be >= 0
SELECT 'DQ-007' AS RuleID, 'High' AS Severity,
       'Sales.Customers' AS [Table], 'CreditLimit' AS [Column],
       'Credit limit cannot be negative' AS RuleDescription,
       COUNT(*) AS FailCount
FROM Sales.Customers WHERE CreditLimit < 0;

-- DQ-008 [Medium] Application.StateProvinces: ProvinceCode must be 2 chars
SELECT 'DQ-008' AS RuleID, 'Medium' AS Severity,
       'Application.StateProvinces' AS [Table], 'StateProvinceCode' AS [Column],
       'Province code must be exactly 2 characters' AS RuleDescription,
       COUNT(*) AS FailCount
FROM Application.StateProvinces WHERE LEN(StateProvinceCode) <> 2;

-- DQ-009 [High] Purchasing.PurchaseOrderLines: UnitCost must be > 0
SELECT 'DQ-009' AS RuleID, 'High' AS Severity,
       'Purchasing.PurchaseOrderLines' AS [Table], 'UnitCost' AS [Column],
       'Unit cost must be positive' AS RuleDescription,
       COUNT(*) AS FailCount
FROM Purchasing.PurchaseOrderLines WHERE UnitCost <= 0;

-- DQ-010 [Medium] Sales.Return: ReturnDate must be >= InvoiceDate
SELECT 'DQ-010' AS RuleID, 'Medium' AS Severity,
       'Sales.Return' AS [Table], 'ReturnDate' AS [Column],
       'Return date must not precede invoice date' AS RuleDescription,
       COUNT(*) AS FailCount
FROM Sales.[Return] r
INNER JOIN Sales.OrderLines ol  ON ol.OrderLineID = r.OrderLineID
INNER JOIN Sales.Orders o       ON o.OrderID      = ol.OrderID
INNER JOIN Sales.Invoices i     ON i.OrderID      = o.OrderID
WHERE r.ReturnDate < i.InvoiceDate;
```

### 4.3 ETL Error Handling Strategy

Once data quality rules are defined, the ETL must implement a strategy for handling violations. There are three fundamental approaches:

**Fail fast.** The package detects a quality violation and stops immediately. The DW is not updated. Operations are alerted and the source data must be corrected before the next load attempt.

*When to use:* High-severity violations where partial loading would produce misleading results.

**Reject and continue.** Rows that violate quality rules are redirected to an error table and excluded from the main load. The load completes with clean rows; rejected rows are logged for investigation and remediation.

*When to use:* Medium-severity violations where most rows are clean and a partial load is preferable to no load.

**Substitute and continue.** The ETL substitutes a default or derived value for the missing or invalid one and logs the substitution. The load completes; substituted values are flagged for review.

*When to use:* Low-severity violations or cases where the business has agreed on a specific default (e.g., `ISNULL(ProductCode, 'No Code')`).

In SSIS, these strategies are implemented using the **Conditional Split** transformation and **Error Outputs**:

```
OLE DB Source
    ↓
Conditional Split
    ├── [Valid rows] UnitPrice > 0 AND Quantity > 0 → main flow → OLE DB Destination
    └── [Invalid rows] otherwise → Error OLE DB Destination (error log table)
```

---

## 5. Source-to-Target Mapping

The **source-to-target (S2T) mapping** is the core ETL design document. It is a complete specification — typically maintained as a spreadsheet — that describes every column in the target, where it comes from, and how it is transformed.

An S2T mapping serves three purposes simultaneously:
1. **Specification** — tells the ETL developer exactly what to build
2. **Test oracle** — defines what correct output looks like, enabling automated reconciliation
3. **Documentation** — answers the question "why does this column have this value?" for the life of the system

### 5.1 S2T Mapping Structure

A complete S2T mapping has the following columns for each target column:

| Column | Description |
|---|---|
| **Target Schema** | Schema in the DW or data mart (e.g., `Dimension`, `Fact`, `fact`, `dim`) |
| **Target Table** | Table name |
| **Target Column** | Column name in the target |
| **Target Data Type** | Data type and precision (e.g., `NVARCHAR(100)`, `DECIMAL(18,2)`) |
| **Nullable** | Y or N |
| **Source Database** | Which database the source data comes from |
| **Source Schema** | Schema in the source database |
| **Source Table** | Primary source table |
| **Source Column** | Column name in the source |
| **Transformation Rule** | Any derivation, lookup, join, conversion, or cleansing applied |
| **Default Value** | Value to use when source is NULL or not available |
| **DQ Rule Reference** | Which DQ rule(s) apply to this column |
| **Notes** | Special handling, business rules, known issues |

### 5.2 Transformation Rule Types

The transformation rule column is the most important part of the mapping. It must be precise enough that a developer can implement it without asking additional questions. Common rule types:

| Rule type | Notation | Example |
|---|---|---|
| **Direct copy** | `Direct` | `CustomerName → CustomerName` |
| **Rename** | `Rename: [source name]` | `StateProvinceName → ProvinceName` |
| **JOIN** | `JOIN [table] ON [condition]` | `JOIN Application.Cities ON CityID = DeliveryCityID` |
| **Lookup** | `LOOKUP [DW table] ON [natural key] → [surrogate key]` | `LOOKUP DimCustomer ON CustomerID → CustomerKey` |
| **Derivation** | `DERIVE: [expression]` | `DERIVE: CASE ProvinceCode WHEN 'NS' THEN 'Atlantic — Nova Scotia' ...` |
| **Conversion** | `CONVERT: [source type] → [target type]` | `CONVERT: DATE → INT YYYYMMDD` |
| **Aggregation** | `AGG: [function]` | `AGG: MIN(ProductCategoryID) → PrimaryCategoryID` |
| **Default** | `DEFAULT: [value]` | `DEFAULT: 0` |
| **NULL handling** | `ISNULL([column], [default])` | `ISNULL(ProductCode, 'No Code')` |
| **Computed** | `COMPUTE: [expression]` | `COMPUTE: ROUND(UnitPrice * StandardCostPct * Quantity, 2)` |
| **Identity** | `IDENTITY` | Surrogate key generated by DW |

### 5.3 Load Order and Dependencies

The S2T mapping must also document **load order dependencies** — which tables must be loaded before others.

In a dimensional model, the dependency chain is:

```
1. Static lookups (DimDeliveryMethod, DimTransactionType, DimPaymentMethod)
2. Date dimension (DimDate — no source dependencies)
3. Geography (DimGeography — no other DW dim dependencies)
4. Customers, Suppliers, Employees (depend on DimGeography)
5. Product Categories (no dependencies)
6. Products (depend on DimProductCategory, DimSupplier)
7. All Fact tables (depend on all their dimensions)
```

Documenting this in the S2T mapping prevents the most common ETL deployment failure: loading facts before their dimensions.

---

## 6. Worked Example: Mapping DimCustomer

`Dimension.DimCustomer` in `CabotTrailOutdoorDW` is built from three OLTP source tables. The complete S2T mapping follows.

### 6.1 Source Tables Involved

```sql
-- Identify all source tables for DimCustomer
USE CabotTrailOutdoor;

-- Primary source
SELECT TOP 3 * FROM Sales.Customers;

-- Geography level 1: City
SELECT TOP 3 * FROM Application.Cities;

-- Geography level 2: Province
SELECT TOP 3 * FROM Application.StateProvinces;

-- Geography level 3: Country
SELECT TOP 3 * FROM Application.Countries;
```

### 6.2 Gap Analysis: DimCustomer

Before mapping, identify all gaps:

| Gap ID | Type | Description | Resolution |
|---|---|---|---|
| GAP-C01 | Structural | Customer geography is split across 4 tables | JOIN all 4 at extract time |
| GAP-C02 | Missing | No `SalesTerritory` attribute in any source table | Derive from `StateProvinceCode` using CASE |
| GAP-C03 | Missing | No `PostalCode` in `Application.Cities` | NULL default; document as known gap |
| GAP-C04 | Missing | `IsOnCreditHold` not loaded into DW from OLTP | Default to 0; note that OLTP has this column |
| GAP-C05 | Structural | DW is Type 1 (no history); datamart requires `ValidFrom`/`ValidTo`/`IsCurrent` | Populate with static defaults: ValidFrom=first load date, ValidTo=9999-12-31, IsCurrent=1 |
| GAP-C06 | Structural | `CustomerGroupName` in OLTP maps to `CustomerCategoryName` in DW | Rename during ETL |

### 6.3 Complete S2T Mapping: DimCustomer

| Target Column | Type | Nullable | Source DB | Source Schema | Source Table | Source Column | Transformation Rule | Default | DQ Rule |
|---|---|---|---|---|---|---|---|---|---|
| `CustomerKey` | INT | N | — | — | — | — | IDENTITY | — | — |
| `CustomerID` | INT | N | CabotTrailOutdoor | Sales | Customers | CustomerID | Direct | — | DQ-001 |
| `CustomerName` | NVARCHAR(100) | N | CabotTrailOutdoor | Sales | Customers | CustomerName | Direct | — | DQ-002 |
| `CustomerCategoryName` | NVARCHAR(50) | N | CabotTrailOutdoor | Sales | Customers | CustomerGroupName | Rename: CustomerGroupName | — | — |
| `CreditLimit` | DECIMAL(18,2) | N | CabotTrailOutdoor | Sales | Customers | CreditLimit | Direct | 0 | DQ-007 |
| `AccountOpenedDate` | DATE | Y | CabotTrailOutdoor | Sales | Customers | AccountOpenedDate | Direct | NULL | — |
| `IsOnCreditHold` | BIT | N | — | — | — | — | DEFAULT: 0 (not in DW source) | 0 | GAP-C04 |
| `CityName` | NVARCHAR(60) | N | CabotTrailOutdoor | Application | Cities | CityName | JOIN Cities ON Customers.DeliveryCityID = Cities.CityID | — | — |
| `ProvinceName` | NVARCHAR(60) | N | CabotTrailOutdoor | Application | StateProvinces | StateProvinceName | JOIN StateProvinces ON Cities.StateProvinceID | — | — |
| `ProvinceCode` | NCHAR(2) | N | CabotTrailOutdoor | Application | StateProvinces | StateProvinceCode | JOIN StateProvinces | — | DQ-008 |
| `CountryName` | NVARCHAR(60) | N | CabotTrailOutdoor | Application | Countries | CountryName | JOIN Countries ON StateProvinces.CountryID | 'Canada' | — |
| `PostalCode` | NVARCHAR(10) | Y | — | — | — | — | DEFAULT: NULL (GAP-C03) | NULL | — |
| `SalesTerritory` | NVARCHAR(60) | Y | — | — | — | — | DERIVE: CASE ProvinceCode WHEN 'NS' THEN 'Atlantic — Nova Scotia' ... | 'Other' | GAP-C02 |
| `ValidFrom` | DATE | N | — | — | — | — | DEFAULT: first ETL load date | '2022-01-01' | GAP-C05 |
| `ValidTo` | DATE | N | — | — | — | — | DEFAULT: 9999-12-31 | '9999-12-31' | GAP-C05 |
| `IsCurrent` | BIT | N | — | — | — | — | DEFAULT: 1 | 1 | GAP-C05 |

### 6.4 Extract Query for DimCustomer

The S2T mapping directly drives the extract query. Every JOIN, rename, and derivation in the mapping is implemented here:

```sql
-- Extract query for DimCustomer — driven by the S2T mapping above
USE CabotTrailOutdoor;

SELECT
    c.CustomerID,
    c.CustomerName,

    -- GAP-C06: Rename CustomerGroupName → CustomerCategoryName
    c.CustomerGroupName                         AS CustomerCategoryName,

    c.CreditLimit,
    c.AccountOpenedDate,

    -- GAP-C04: IsOnCreditHold not in DW source; default to 0
    0                                           AS IsOnCreditHold,

    -- GAP-C01: JOIN geography hierarchy (structural gap)
    ci.CityName,
    sp.StateProvinceName                        AS ProvinceName,
    sp.StateProvinceCode                        AS ProvinceCode,
    co.CountryName,

    -- GAP-C03: PostalCode not available; NULL default
    NULL                                        AS PostalCode,

    -- GAP-C02: SalesTerritory derived from ProvinceCode (missing data gap)
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
    END                                         AS SalesTerritory,

    -- GAP-C05: SCD static defaults
    CAST('2022-01-01' AS DATE)                  AS ValidFrom,
    CAST('9999-12-31' AS DATE)                  AS ValidTo,
    1                                           AS IsCurrent

FROM    Sales.Customers c

-- GAP-C01: JOIN to resolve geography
INNER JOIN Application.Cities ci
    ON ci.CityID = c.DeliveryCityID
INNER JOIN Application.StateProvinces sp
    ON sp.StateProvinceID = ci.StateProvinceID
INNER JOIN Application.Countries co
    ON co.CountryID = sp.CountryID

-- Exclude the unknown member
WHERE   c.CustomerID <> 0;
```

> Notice how every line of this query can be traced back to a row in the S2T mapping table. The mapping is not just documentation — it is the specification from which the code is derived. If the mapping changes, the code changes in a predictable, documented way.

---

## 7. Worked Example: Mapping FactSales

Fact table mapping is more complex than dimension mapping because it involves multiple lookups, derived measures, and a clearly defined grain.

### 7.1 Grain Declaration

Before mapping any columns, declare the grain:

> **Grain:** One row per product on each customer invoice — representing the quantity ordered, quantity picked, unit price, tax, line total, unit cost (derived from `StandardCostPct`), gross profit, and gross profit margin for a single product on a single invoice.

### 7.2 Source Tables Involved

`Fact.FactSales` is built from seven source tables:

| Source table | Role |
|---|---|
| `Sales.InvoiceLines` | Primary source — line-level measures |
| `Sales.Invoices` | Invoice date, due date, delivery method, sales rep |
| `Sales.Orders` | Order date, city |
| `Sales.OrderLines` | Ordered quantity, picked quantity |
| `Sales.Customers` | Customer → used for CustomerKey lookup |
| `Inventory.ProductCategoryAssignments` | Primary category → for cost rate lookup |
| `Inventory.ProductCategories` | `StandardCostPct` → for UnitCost derivation |

### 7.3 Gap Analysis: FactSales

| Gap ID | Type | Description | Resolution |
|---|---|---|---|
| GAP-F01 | Structural | `CustomerKey` required but source has `CustomerID` | LOOKUP DimCustomer on CustomerID → CustomerKey |
| GAP-F02 | Structural | `ProductKey` required but source has `ProductID` | LOOKUP DimProduct on ProductID → ProductKey |
| GAP-F03 | Structural | `EmployeeKey` required but source has `SalesRepID` | LOOKUP DimEmployee on EmployeeID → EmployeeKey |
| GAP-F04 | Structural | `GeographyKey` required but source has `CityID` | LOOKUP DimGeography on CityID → GeographyKey |
| GAP-F05 | Structural | `DeliveryMethodKey` required but source has `DeliveryMethodID` | LOOKUP DimDeliveryMethod on DeliveryMethodID → DeliveryMethodKey |
| GAP-F06 | Structural | Date keys must be YYYYMMDD integers; source has DATE columns | CONVERT: `CAST(FORMAT(date, 'yyyyMMdd') AS INT)` |
| GAP-F07 | Missing | `UnitCost` not on `InvoiceLines`; must be derived | COMPUTE: `UnitPrice × StandardCostPct × PickedQuantity` |
| GAP-F08 | Missing | `GrossProfit` not on `InvoiceLines` | COMPUTE: `LineTotal - UnitCost` |
| GAP-F09 | Missing | `GrossProfitMarginPct` not on `InvoiceLines` | COMPUTE: `GrossProfit / LineTotal × 100` (NULLIF guard) |
| GAP-F10 | Structural | `LineTotalIncludingTax` is a computed column in OLTP; not selectable by name | COMPUTE: `LineTotal + TaxAmount` |
| GAP-F11 | Structural | M:M category relationship; cost rate requires primary category | AGG: `MIN(ProductCategoryID)` per ProductID |
| GAP-F12 | Missing | Exclude 2026 open orders (no invoices) | FILTER: `YEAR(OrderDate) < 2026` |

### 7.4 Key Measures: Derivation Rules

The three most important derived measures in `FactSales` deserve explicit documentation of their derivation logic:

**UnitCost:**
```
Source:   il.UnitPrice (from Sales.InvoiceLines)
          × pcat.StandardCostPct (from Inventory.ProductCategories via primary category)
          × ol.PickedQuantity (from Sales.OrderLines)
Formula:  ROUND(il.UnitPrice * pcat.StandardCostPct * ol.PickedQuantity, 2)
Rationale: Represents the total cost of goods for the picked quantity of this line.
           StandardCostPct varies by product category (range: 0.45 to 0.70),
           producing meaningful margin variation across the product catalogue.
```

**GrossProfit:**
```
Source:   il.LineTotal - UnitCost (as derived above)
Formula:  ROUND(il.LineTotal - (il.UnitPrice * pcat.StandardCostPct * ol.PickedQuantity), 2)
Rationale: Revenue minus cost for this invoice line.
           Additive: can be summed across any dimension.
```

**GrossProfitMarginPct:**
```
Source:   GrossProfit / LineTotal × 100
Formula:  CASE WHEN il.LineTotal = 0 THEN 0
               ELSE ROUND((il.LineTotal - (il.UnitPrice * pcat.StandardCostPct * ol.PickedQuantity))
                          / il.LineTotal * 100, 2)
          END
Rationale: Percentage margin for this line.
           NON-ADDITIVE: never sum this column. Always compute from GrossProfit / LineTotal.
           Stored for convenience only.
```

### 7.5 Complete Extract Query for FactSales

```sql
-- FactSales extract query — implements all gaps from the mapping
USE CabotTrailOutdoor;

SELECT
    -- GAP-F06: Convert date columns to YYYYMMDD integer keys
    CAST(FORMAT(o.OrderDate,   'yyyyMMdd') AS INT)          AS OrderDateKey,
    CAST(FORMAT(i.InvoiceDate, 'yyyyMMdd') AS INT)          AS InvoiceDateKey,
    CAST(FORMAT(i.DueDate,     'yyyyMMdd') AS INT)          AS DueDateKey,

    -- GAP-F01 to F05: Natural keys for SSIS Lookup transformations
    -- These are replaced by surrogate keys during the SSIS Lookup stage
    c.CustomerID,
    il.ProductID,
    i.SalesRepID                                            AS EmployeeID,
    o.CityID,
    i.DeliveryMethodID,

    -- Degenerate dimensions
    i.InvoiceID,
    o.OrderID,
    il.OrderLineID,

    -- Direct measures
    ol.Quantity                                             AS OrderedQuantity,
    ol.PickedQuantity,
    il.UnitPrice,
    il.TaxRate,
    il.LineTotal,
    il.TaxAmount,

    -- GAP-F10: LineTotalIncludingTax computed inline
    ROUND(il.LineTotal + il.TaxAmount, 2)                   AS LineTotalIncludingTax,

    -- GAP-F07: UnitCost derived from StandardCostPct
    ROUND(il.UnitPrice * pcat.StandardCostPct
          * ol.PickedQuantity, 2)                           AS UnitCost,

    -- GAP-F08: GrossProfit
    ROUND(il.LineTotal
          - (il.UnitPrice * pcat.StandardCostPct
             * ol.PickedQuantity), 2)                       AS GrossProfit,

    -- GAP-F09: GrossProfitMarginPct (non-additive — stored for convenience)
    CASE
        WHEN il.LineTotal = 0 THEN 0
        ELSE ROUND(
            (il.LineTotal - (il.UnitPrice * pcat.StandardCostPct * ol.PickedQuantity))
            / il.LineTotal * 100, 2)
    END                                                     AS GrossProfitMarginPct

FROM    Sales.InvoiceLines il
INNER JOIN Sales.Invoices i     ON i.InvoiceID   = il.InvoiceID
INNER JOIN Sales.Orders o       ON o.OrderID     = i.OrderID
INNER JOIN Sales.OrderLines ol  ON ol.OrderLineID = il.OrderLineID
INNER JOIN Sales.Customers c    ON c.CustomerID  = i.CustomerID

-- GAP-F11: Primary category via MIN aggregation (resolve M:M)
INNER JOIN (
    SELECT  pca.ProductID,
            MIN(pca.ProductCategoryID)  AS PrimaryCategoryID
    FROM    Inventory.ProductCategoryAssignments pca
    GROUP BY pca.ProductID
) pri ON pri.ProductID = il.ProductID

-- GAP-F07: StandardCostPct from primary category
INNER JOIN Inventory.ProductCategories pcat
    ON pcat.ProductCategoryID = pri.PrimaryCategoryID

-- GAP-F12: Exclude open orders (2026) with no invoices
WHERE   YEAR(o.OrderDate) < 2026;
```

This extract query produces every row and column needed for `FactSales`, with surrogate key lookups remaining to be resolved in the SSIS Data Flow (covered in Chapter 4).

---

## 8. The Relationship Between Mapping and Implementation

The S2T mapping and the ETL implementation should maintain a one-to-one correspondence throughout the life of the system. This discipline is what makes ETL systems maintainable.

### 8.1 Traceability

Every column in the target should be traceable through the S2T mapping to its source. Every transformation in an SSIS package should have a corresponding row in the S2T mapping that describes what it does and why.

When a business analyst asks "why does this customer's territory show as 'Other' instead of 'Atlantic — Nova Scotia'?", the trace is:
1. `SalesTerritory` in `dim.Customer` → S2T mapping row → Transformation Rule: CASE on `ProvinceCode`
2. `ProvinceCode` in `dim.Customer` → S2T mapping row → Source: `Application.StateProvinces.StateProvinceCode`
3. Check: is `StateProvinceCode` populated for this customer? If not, CASE falls to `ELSE 'Other'`

Without the mapping, this trace requires reading SSIS package expressions — slow, error-prone, and inaccessible to non-developers.

### 8.2 Change Management

When a business rule changes — for example, Quebec is split into two territories ("Quebec East" and "Quebec West") — the change process is:
1. Update the S2T mapping (modify the CASE expression in the `SalesTerritory` derivation rule)
2. Update the SSIS Derived Column expression to match
3. Test that the new rule produces the expected output
4. Deploy and run the updated ETL

Without the mapping, step 1 is skipped — the change is made directly in the package. The next developer who looks at the package has no idea the territory logic was changed, when, or why.

### 8.3 The Mapping as a Test Specification

The S2T mapping can be directly translated into automated reconciliation tests:

```sql
-- Test derived from S2T mapping: DimCustomer SalesTerritory derivation
-- Expected: all NS customers have SalesTerritory = 'Atlantic — Nova Scotia'
SELECT  COUNT(*) AS FailCount
FROM    CabotTrailOutdoorDW.Dimension.DimCustomer
WHERE   ProvinceCode = 'NS'
AND     SalesTerritory <> 'Atlantic — Nova Scotia'
AND     CustomerID <> 0;
-- Should return 0

-- Test derived from S2T mapping: FactSales GrossProfit derivation
-- Expected: stored GrossProfit matches computed value from LineTotal and UnitCost
SELECT  COUNT(*) AS DerivationMismatches
FROM    CabotTrailOutdoorDW.Fact.FactSales
WHERE   ABS(GrossProfit - (LineTotal - UnitCost)) > 0.01  -- Allow $0.01 rounding tolerance
AND     OrderDateKey > 0;
-- Should return 0
```

These tests are not ad hoc — they flow directly from the documented transformation rules. The S2T mapping is the specification; the tests verify conformance to the specification.

---

## 9. Chapter Summary

- **Gap analysis** identifies the differences between source and target before ETL development begins. There are three gap types: **missing data** (target needs something the source does not have), **data quality** (source data exists but is dirty), and **structural** (data exists but is organized differently).

- **Source data analysis** profiles source tables systematically — row counts, column distributions, NULL rates, value ranges, and referential integrity. It must be completed before ETL design begins.

- **Data quality assessment** documents quality rules as a formal table with rule ID, severity (High/Medium/Low), SQL measurement, and pass condition. Rules drive ETL error handling strategy: fail fast, reject and continue, or substitute and continue.

- The **source-to-target mapping** is the core ETL design document. It specifies every target column, its source, and its transformation rule. It drives implementation, enables testing, and supports change management.

- The **extract query** is derived directly from the S2T mapping. Every JOIN, rename, derivation, and conversion in the mapping appears as a corresponding SQL construct in the extract query.

- The S2T mapping must be **maintained alongside the implementation** throughout the life of the ETL system. It is the bridge between the business rules and the technical implementation.

---

## 10. Review Questions

1. A new dimension table `DimPromotion` is being added to the data warehouse. The source system stores promotions in a table `Marketing.Promotions` with columns `PromotionID`, `PromotionName`, `StartDate`, `EndDate`, and `DiscountPctText` (a NVARCHAR column that stores values like '15%' or '10.5%'). Identify the gap type(s) present and describe the transformation rule(s) needed.

2. During source data analysis of `Sales.InvoiceLines`, you find that `UnitPrice <= 0` for 3 rows out of 16,359. You have defined this as a High-severity data quality rule (DQ-003). Describe the two ETL error handling strategies available and give a specific reason to choose one over the other for this scenario.

3. Explain why referential integrity checks against the source data are particularly important for ETL that uses SSIS Lookup transformations. What happens in SSIS when a lookup key is not found, and what are the two ways to handle this?

4. The S2T mapping for `DimProduct` shows `ProductCategoryKey` with transformation rule `LOOKUP DimProductCategory ON ProductCategoryID → ProductCategoryKey`. What must be true about `DimProductCategory` before the `DimProduct` load can run? What happens if the sequence is reversed?

5. `GrossProfitMarginPct` is stored in `Fact.FactSales` with the S2T mapping noting it is non-additive. A developer writes a report that shows `SUM(GrossProfitMarginPct)` grouped by `CalendarYear`. What is wrong with this, and what should the query do instead?

6. The `SalesTerritory` derivation in `DimCustomer` uses a CASE expression on `ProvinceCode`. Six months after deployment, the business decides that Manitoba and Saskatchewan should be separated (currently both show as 'Prairies'). Walk through the complete change management process, starting with the S2T mapping.

7. A source data analysis reveals that `Purchasing.PurchaseOrders.ActualDeliveryDate` is NULL for 15% of rows — those are open orders not yet delivered. Is this a data quality gap? How should it be handled in the S2T mapping for `Fact.FactPurchasing`?

8. Why is it important to run referential integrity checks against the *source* data before loading the *target*? Give a specific example from the CabotTrail environment where an RI failure in the source would cause a specific failure in the ETL load.

---

## 🔍 Deeper Dive

### Going Further with Gap Analysis and S2T Mapping

#### Data Profiling at Scale

The source data analysis queries shown in this chapter are effective for tables with thousands to hundreds of thousands of rows. At enterprise scale — billions of rows — running `COUNT(*)`, `MIN()`, `MAX()`, and `COUNT(DISTINCT)` across entire tables becomes impractical.

Enterprise ETL platforms and data observability tools address this with **statistical sampling** and **incremental profiling**:

- **Statistical sampling** — profile a random sample (1% or 10%) of the table rather than the full dataset. For normally distributed data, this produces accurate statistics at a fraction of the cost.

- **Incremental profiling** — rather than profiling the full table on every run, profile only the rows that changed since the last profile run (using a watermark timestamp). This keeps profiling overhead constant regardless of table size.

Microsoft SQL Server's **Data Quality Services (DQS)** provides a managed data quality platform with built-in profiling, rule definition, and cleansing workflows. While DQS is beyond the scope of this book, it represents the enterprise-scale evolution of the manual DQ approach described here:
[SQL Server Data Quality Services](https://learn.microsoft.com/en-us/sql/data-quality-services/data-quality-services)

#### Data Lineage and Impact Analysis

The S2T mapping described in this chapter is a manual documentation artifact. Enterprise data management platforms automate lineage tracking — recording, at runtime, which source rows contributed to which target rows, through which transformation steps.

**Data lineage** answers "where did this data come from?" Impact analysis (the inverse) answers "if I change this source column, what downstream targets are affected?"

Tools that provide automated lineage in SQL Server environments include:
- **SQL Server Integration Services** — the SSIS Catalog (`SSISDB`) records package execution history but not column-level lineage
- **Microsoft Purview** — Microsoft's enterprise data governance platform with automated lineage across Azure data services
- **Apache Atlas** — open-source metadata management with lineage tracking
- **Informatica Data Catalog** — commercial metadata and lineage platform

Microsoft Purview documentation: [Microsoft Purview data governance documentation](https://learn.microsoft.com/en-us/purview/)

#### The Data Vault Approach to Source Analysis

The **Data Vault 2.0** methodology (Dan Linstedt) takes a different approach to the source-to-target problem. Rather than mapping sources to a pre-designed dimensional model, Data Vault loads raw source data into a highly normalized integration layer (Hubs, Links, and Satellites), preserving all source data exactly as received. Transformation to a dimensional model happens in a second layer (the "Information Mart").

This approach changes the nature of gap analysis: instead of asking "what does the target need?", Data Vault asks "what does the source have?" and loads everything. Dimensional model decisions are deferred to the reporting layer.

Data Vault is particularly effective when:
- Multiple heterogeneous sources need to be integrated
- Source schemas change frequently
- Full auditability of every source record is required

For contexts where the source is stable and the target model is well-defined (like CabotTrail), Kimball's approach is more efficient. Understanding both helps practitioners choose the right methodology for a given project.

Linstedt, D., & Olschimke, M. (2015). *Building a Scalable Data Warehouse with Data Vault 2.0*. Morgan Kaufmann.

#### Formal Data Quality Frameworks

The data quality dimensions described in section 4.1 (completeness, accuracy, consistency, etc.) are drawn from a formal body of literature on data quality management. The most widely referenced framework is the **DAMA-DMBOK** (Data Management Body of Knowledge), which defines data quality as one of eleven data management disciplines:

DAMA International. (2017). *DAMA-DMBOK: Data Management Body of Knowledge* (2nd ed.). Technics Publications.

The **ISO 8000** standard provides international specifications for data quality, particularly for master data and transaction data in supply chain contexts. While ISO 8000 is primarily relevant to large enterprises, understanding its existence positions practitioners in the broader context of data governance.

For practitioners building production ETL systems, the **TDWI (Transforming Data with Intelligence)** organization provides research, training, and best practices specifically for data warehousing and BI:
[TDWI — Data Quality Resources](https://tdwi.org/home.aspx)

#### S2T Mapping Tools

While spreadsheet-based S2T mappings (Excel or Google Sheets) are the most common format in practice, several tools exist for managing mappings at scale:

- **WhereScape** — metadata-driven ETL automation platform that generates SSIS packages from S2T mapping metadata
- **Manta** — automated data lineage and impact analysis from SQL code
- **Talend Data Catalog** — metadata management with S2T documentation support
- **dbt (Data Build Tool)** — for ELT architectures, dbt's documentation layer provides S2T mapping functionality built into the transformation code

The principle remains the same across all tools: the mapping is the specification, and the specification must be maintained alongside the implementation.

---

### Industry Perspectives

#### Kimball on Source System Analysis

Kimball dedicates significant attention in *The Data Warehouse Toolkit* to the importance of understanding source data before designing the dimensional model. His guidance is characteristically direct:

> *"The single biggest mistake a data warehouse team can make is to not fully understand the source data before designing the data warehouse. You will be blindsided by data quality issues, structural anomalies, and missing data — and you will discover them in production, not in development."*

The source data analysis work described in this chapter is not optional pre-work — it is a core design activity. Many ETL project failures can be traced to teams that began implementation before source analysis was complete and discovered late-breaking data quality issues that required redesigning already-built packages.

#### Microsoft on Data Quality in SSIS

Microsoft's documentation for SSIS error handling covers both Conditional Split and Error Output in depth:
- [SSIS — Error Handling in Data](https://learn.microsoft.com/en-us/sql/integration-services/data-flow/error-handling-in-data)
- [SSIS — Conditional Split Transformation](https://learn.microsoft.com/en-us/sql/integration-services/data-flow/transformations/conditional-split-transformation)

The SSIS Data Profiling Task (available in the Control Flow) provides automated column profiling:
[SSIS — Data Profiling Task](https://learn.microsoft.com/en-us/sql/integration-services/control-flow/data-profiling-task)

---

### References and Further Reading

1. Kimball, R., & Ross, M. (2013). *The Data Warehouse Toolkit: The Definitive Guide to Dimensional Modeling* (3rd ed.). Wiley. — Chapter 16 covers ETL design patterns and source data analysis in detail.

2. Kimball, R., Reeves, L., Ross, M., & Thornthwaite, W. (2004). *The Data Warehouse ETL Toolkit*. Wiley. — The companion to the Toolkit, focused entirely on ETL design and implementation. Chapter 3 covers source data analysis; Chapter 4 covers data quality.

3. DAMA International. (2017). *DAMA-DMBOK: Data Management Body of Knowledge* (2nd ed.). Technics Publications. — Chapter 13 defines the data quality dimensions framework.

4. Linstedt, D., & Olschimke, M. (2015). *Building a Scalable Data Warehouse with Data Vault 2.0*. Morgan Kaufmann. — Alternative approach to source-to-target design; useful comparative context.

5. Microsoft. (2024). *Error Handling in Data — SSIS*. [https://learn.microsoft.com/en-us/sql/integration-services/data-flow/error-handling-in-data](https://learn.microsoft.com/en-us/sql/integration-services/data-flow/error-handling-in-data)

6. Microsoft. (2024). *Data Profiling Task — SSIS*. [https://learn.microsoft.com/en-us/sql/integration-services/control-flow/data-profiling-task](https://learn.microsoft.com/en-us/sql/integration-services/control-flow/data-profiling-task)

7. Microsoft. (2024). *SQL Server Data Quality Services*. [https://learn.microsoft.com/en-us/sql/data-quality-services/data-quality-services](https://learn.microsoft.com/en-us/sql/data-quality-services/data-quality-services)

8. Microsoft. (2024). *Microsoft Purview data governance documentation*. [https://learn.microsoft.com/en-us/purview/](https://learn.microsoft.com/en-us/purview/)

9. TDWI. (n.d.). *Data Quality Resources*. [https://tdwi.org/home.aspx](https://tdwi.org/home.aspx)

10. Redman, T. C. (2001). *Data Quality: The Field Guide*. Digital Press. — A practitioner-focused treatment of data quality management principles that complements the technical ETL approach.

---

*Previous chapter: [Chapter 2 — Dimensional Modelling: Stars, Schemas, and the Language of Analytics](../chapter-02-dimensional-modelling/README.md)*

*Next chapter: [Chapter 4 — ETL Implementation: Dimensions, Lookups, and Derivations](../chapter-04-etl-implementation/README.md)*

---

> **ETL for Business Intelligence** | © Patrick Dolinger, NSCC Institute of Technology
> [CC BY 4.0](https://creativecommons.org/licenses/by/4.0/) — Share and adapt freely with attribution
