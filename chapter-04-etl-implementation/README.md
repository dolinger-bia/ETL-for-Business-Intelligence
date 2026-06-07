# Chapter 4: ETL Implementation — Dimensions, Lookups, and Derivations

> **ETL for Business Intelligence**
> *A practical guide to data provisioning, dimensional modelling, and pipeline design*
>
> © Patrick Dolinger, NSCC Institute of Technology
> Licensed under [Creative Commons Attribution 4.0 International (CC BY 4.0)](https://creativecommons.org/licenses/by/4.0/)
> You are free to share and adapt this material for any purpose, provided appropriate credit is given.

---

## Chapter Overview

Chapters 1 through 3 covered the *what* and *why* of ETL — the architecture, the dimensional model, and the design specifications that precede implementation. This chapter crosses the line into the *how*: building actual ETL pipelines in SQL Server Integration Services.

The S2T mapping from Chapter 3 is the specification. This chapter is the implementation guide. Every SSIS component introduced here maps directly to a transformation rule type from the mapping. By the end of this chapter, you will be able to translate a completed S2T mapping into a working, tested SSIS solution.

By the end of this chapter you will be able to:

- Describe the SSIS Control Flow and Data Flow and explain the purpose of each
- Build a dimension load package using OLE DB Source, Lookup, Derived Column, and OLE DB Destination
- Implement surrogate key resolution using the SSIS Lookup transformation
- Apply the Conditional Split transformation to implement data quality error handling
- Build a fact table load package that implements all lookup types identified in the Chapter 3 S2T mapping
- Write and execute reconciliation queries that verify ETL correctness
- Explain the load dependency order for a dimensional model and implement it in a master package

---

## Table of Contents

1. [SSIS Architecture: Control Flow and Data Flow](#1-ssis-architecture-control-flow-and-data-flow)
2. [The OLE DB Source](#2-the-ole-db-source)
3. [The Lookup Transformation](#3-the-lookup-transformation)
4. [The Derived Column Transformation](#4-the-derived-column-transformation)
5. [The Conditional Split Transformation](#5-the-conditional-split-transformation)
6. [The OLE DB Destination](#6-the-ole-db-destination)
7. [Building a Dimension Load Package](#7-building-a-dimension-load-package)
8. [Building a Fact Table Load Package](#8-building-a-fact-table-load-package)
9. [Reconciliation and Verification](#9-reconciliation-and-verification)
10. [The Master Package and Load Order](#10-the-master-package-and-load-order)
11. [Chapter Summary](#11-chapter-summary)
12. [Review Questions](#12-review-questions)
13. [🔍 Deeper Dive](#-deeper-dive)

---

## 1. SSIS Architecture: Control Flow and Data Flow

An SSIS package has two distinct execution environments: the **Control Flow** and the **Data Flow**. Understanding the separation between them is the foundation for designing packages that are correct, maintainable, and performant.

### 1.1 The Control Flow

The Control Flow is the **orchestration layer**. It determines what runs, in what order, under what conditions, and what to do when something fails. It contains tasks and containers connected by precedence constraints.

Think of the Control Flow as the conductor of an orchestra — it does not play any instruments itself, but it directs everything that does.

```
Control Flow of a typical dimension load package:

[Truncate DimCustomer]         ← Execute SQL Task
        ↓ Success
[Load DimCustomer]             ← Data Flow Task
        ↓ Success
[Run Reconciliation Tests]     ← Execute SQL Task
        ↓ Failure
[Log Error to ETL.TestResults] ← Execute SQL Task
```

**Key Control Flow tasks:**

| Task | Purpose | Common use cases |
|---|---|---|
| **Execute SQL Task** | Run T-SQL against a connection | Truncate tables, call stored procedures, log results, create temp tables |
| **Data Flow Task** | Execute a data movement pipeline | Any extract-transform-load operation |
| **Execute Package Task** | Call a child package | Master package orchestration |
| **Sequence Container** | Group tasks with shared error handling | Group all dimension loads; group all fact loads |
| **For Loop Container** | Repeat a fixed number of times | Rarely used in ETL; more common in administrative scripts |
| **Foreach Loop Container** | Iterate over a collection | Process multiple files; iterate over a result set |
| **Script Task** | Execute C# or VB.NET code | Complex logic that cannot be expressed in other tasks |

**Precedence constraints** define when a connected task runs:

| Constraint | Condition | Visual |
|---|---|---|
| **Success** | Run next task only if this task succeeded | Green arrow |
| **Failure** | Run next task only if this task failed | Red arrow |
| **Completion** | Run next task regardless of outcome | Blue arrow |
| **Expression** | Run next task if a custom expression evaluates to TRUE | Dashed arrow |

The most important constraint to understand is **Failure**. In production ETL, failure paths are as important as success paths. A package that silently swallows errors and reports success is dangerous. Every critical task should have a failure path that logs the error and alerts operations.

### 1.2 The Data Flow

The Data Flow is the **transformation engine**. It operates as a streaming pipeline — rows flow from a source through a series of transformations to a destination. Unlike the Control Flow, which runs tasks sequentially, the Data Flow processes rows in parallel — as rows leave the source, they flow into the first transformation without waiting for all rows to be read.

This streaming architecture is fundamental to SSIS performance. A Data Flow pipeline with one million rows does not wait for all one million to be read before starting transformations — rows flow through the entire pipeline continuously, processed in memory buffers.

```
Data Flow pipeline for a dimension load:

[OLE DB Source]              ← Read from OLTP
        ↓ (all rows)
[Derived Column]             ← Compute SalesTerritory, date keys, flags
        ↓
[Lookup: CustomerKey]        ← Resolve surrogate key (only for fact loads)
        ├── Match output ↓
        └── No Match → [Error OLE DB Destination]
[OLE DB Destination]         ← Write to DW dimension table
```

**Key Data Flow components:**

| Component | Type | Purpose |
|---|---|---|
| **OLE DB Source** | Source | Read rows from SQL Server |
| **Flat File Source** | Source | Read rows from CSV or fixed-width files |
| **OLE DB Destination** | Destination | Write rows to SQL Server |
| **Lookup** | Transformation | Join to a reference table; resolve keys |
| **Derived Column** | Transformation | Add or replace columns using expressions |
| **Conditional Split** | Transformation | Route rows to different outputs based on a condition |
| **Data Conversion** | Transformation | Change a column's data type |
| **Aggregate** | Transformation | Group and summarize rows |
| **Union All** | Transformation | Combine rows from multiple inputs |
| **Multicast** | Transformation | Send the same rows to multiple outputs |
| **Sort** | Transformation | Order rows (use sparingly — breaks streaming) |
| **Row Count** | Transformation | Count rows passing through; store in a variable |

### 1.3 The Buffer Architecture

The Data Flow's streaming performance comes from its **buffer architecture**. SSIS allocates memory buffers of fixed size (default 10 MB, configurable). Rows from the source fill a buffer; once the buffer is full, it is passed downstream to the next transformation while the source continues filling a new buffer.

This means that at any point during execution, multiple buffers may be in flight simultaneously — one being filled by the source, another being transformed, another being written to the destination. The pipeline processes continuously rather than in discrete steps.

The practical implication: the Data Flow is optimized for throughput (moving many rows efficiently) rather than latency (processing one row as fast as possible). For high-volume dimension and fact loads, the buffer architecture makes SSIS exceptionally fast.

---

## 2. The OLE DB Source

The **OLE DB Source** is the entry point for most ETL Data Flows. It reads rows from a SQL Server table, view, or query result and passes them into the pipeline.

### 2.1 Configuration

The OLE DB Source has three data access modes:

| Mode | Description | When to use |
|---|---|---|
| **Table or view** | Read all rows from a named table or view | Simple lookups; complete table loads |
| **Table or view — fast load** | Same, with performance optimizations | Large tables where all rows are needed |
| **SQL command** | Execute a T-SQL query | Any transformation-at-source (joins, filters, derivations) |
| **SQL command from variable** | Execute a T-SQL query stored in an SSIS variable | Dynamic SQL; parameterized queries |

For ETL loading dimensional models, **SQL command** is almost always the correct choice. The extract query from the S2T mapping (Chapter 3) is entered directly here.

### 2.2 Column Output

After configuring the source query, SSIS automatically detects the output columns and their data types. Review these carefully:

- Verify that date columns are mapped to `DT_DBDATE` or `DT_DBTIMESTAMP`, not `DT_WSTR` (string)
- Verify that decimal columns have the correct precision and scale
- Verify that integer columns are the appropriate size (`DT_I4` for INT, `DT_I2` for SMALLINT, `DT_I1` for TINYINT)

Type mismatches at the source cause failures deep in the pipeline — a type detected incorrectly at the source will fail at the destination, after all transformation work has already been done.

### 2.3 The Extract Query in the OLE DB Source

The extract query is the SQL developed in Chapter 3 — the complete SELECT statement that implements all structural gaps and joins from the S2T mapping. For `DimCustomer`:

```sql
-- Entered in OLE DB Source > SQL Command
SELECT
    c.CustomerID,
    c.CustomerName,
    c.CustomerGroupName                         AS CustomerCategoryName,
    c.CreditLimit,
    c.AccountOpenedDate,
    0                                           AS IsOnCreditHold,
    ci.CityName,
    sp.StateProvinceName                        AS ProvinceName,
    sp.StateProvinceCode                        AS ProvinceCode,
    co.CountryName,
    NULL                                        AS PostalCode,
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
    CAST('2022-01-01' AS DATE)                  AS ValidFrom,
    CAST('9999-12-31' AS DATE)                  AS ValidTo,
    1                                           AS IsCurrent
FROM    Sales.Customers c
INNER JOIN Application.Cities ci
    ON ci.CityID = c.DeliveryCityID
INNER JOIN Application.StateProvinces sp
    ON sp.StateProvinceID = ci.StateProvinceID
INNER JOIN Application.Countries co
    ON co.CountryID = sp.CountryID
WHERE   c.CustomerID <> 0
```

Notice that the `SalesTerritory` derivation from the S2T mapping is implemented entirely in the source SQL — not in a Derived Column transformation. This is a deliberate design choice: **transformations that can be expressed cleanly in SQL belong in SQL**. Moving complex CASE expressions into SSIS Derived Column expressions makes them harder to read, harder to debug, and harder to change.

The general principle: use the extract query for structural transformations (joins, renames, CASE expressions, NULL handling). Use SSIS transformations for operations that genuinely require in-pipeline processing (surrogate key lookups, conditional routing, data type conversions that SQL cannot handle cleanly).

---

## 3. The Lookup Transformation

The **Lookup** transformation is the most important transformation in dimensional ETL. It resolves natural keys from source data to surrogate keys in dimension tables — the core operation that links fact rows to their dimensions.

### 3.1 How the Lookup Works

A Lookup transformation is conceptually equivalent to a SQL LEFT JOIN — it takes each row flowing through the pipeline, searches a reference dataset for a matching row, and adds columns from the matching row to the pipeline row.

```
Pipeline row:  CustomerID = 42, LineTotal = 249.99, ...
Lookup dataset: SELECT CustomerID, CustomerKey FROM DimCustomer
                42 → CustomerKey = 15

Output row: CustomerKey = 15, LineTotal = 249.99, ...
```

The natural key (`CustomerID`) is the join column. The surrogate key (`CustomerKey`) is the output column added to the pipeline. After the lookup, the pipeline carries both the natural key (no longer needed for loading but useful for debugging) and the surrogate key (required as the FK in the fact table).

### 3.2 Lookup Configuration

**Step 1 — Connection:** Set the lookup connection to the DW database.

**Step 2 — Query:** Define the reference dataset — the query that produces the lookup table. Keep it minimal — only the columns needed for the join and the output:

```sql
-- Lookup reference query: DimCustomer
SELECT  CustomerID, CustomerKey
FROM    Dimension.DimCustomer
WHERE   CustomerID <> 0;

-- Lookup reference query: DimProduct
SELECT  ProductID, ProductKey
FROM    Dimension.DimProduct
WHERE   ProductID <> 0;

-- Lookup reference query: DimDate
SELECT  DateKey
FROM    Dimension.DimDate
WHERE   DateKey <> 0;
-- (No output column needed — just validates the key exists)
```

**Step 3 — Join column:** Map the pipeline column to the lookup column.
- Pipeline column: `CustomerID` → Lookup column: `CustomerID`

**Step 4 — Output column:** Select the column(s) to add to the pipeline from the lookup result.
- Add `CustomerKey` to the pipeline output

**Step 5 — No match behaviour:** This is the most important configuration decision.

| No match behaviour | Effect | When to use |
|---|---|---|
| **Fail component** | Any unmatched row fails the entire Data Flow | Never use in production |
| **Ignore failure** | Unmatched rows pass through with NULL for lookup columns | Dangerous — silently loads NULL FKs |
| **Redirect rows to no match output** | Unmatched rows are sent to a separate error output | **Always use this in production** |

The **redirect to no match output** option is the correct approach for all production ETL. Unmatched rows — rows where the natural key has no corresponding dimension record — are redirected to an error destination for investigation. The main load continues with clean rows.

### 3.3 Lookup Caching

By default, SSIS loads the entire lookup reference dataset into memory once and caches it for the duration of the Data Flow. This is **Full Cache mode** — the fastest option for lookups against small to medium reference tables (dimension tables typically qualify).

For very large reference tables (millions of rows), **Partial Cache mode** loads only the rows actually referenced, querying the database for each miss. This uses less memory but generates many small database round-trips, which can be slow.

For extremely large reference tables or when the reference data changes during the ETL run, **No Cache mode** queries the database for every single row — maximum flexibility but lowest performance.

**Recommendation for CabotTrail:** All lookups use Full Cache. The largest CabotTrail dimension table (`DimCustomer`) has 100 rows — trivially small for full cache loading.

### 3.4 Multiple Lookups in a Single Data Flow

A fact table load typically requires multiple Lookup transformations — one for each foreign key. They are chained sequentially in the Data Flow:

```
OLE DB Source (with CustomerID, ProductID, EmployeeID, CityID, DeliveryMethodID)
        ↓
Lookup: CustomerID → CustomerKey
        ↓ Match
Lookup: ProductID → ProductKey
        ↓ Match
Lookup: EmployeeID → EmployeeKey
        ↓ Match
Lookup: CityID → GeographyKey
        ↓ Match
Lookup: DeliveryMethodID → DeliveryMethodKey
        ↓ Match
OLE DB Destination (FactSales)
```

Each lookup adds its surrogate key to the pipeline. No-match outputs from each lookup are redirected to error tables for investigation. By the time rows reach the destination, all five surrogate keys are present and all rows have passed five validation checks.

### 3.5 Date Key Lookups

Date keys in the CabotTrail DW are YYYYMMDD integers computed in the extract query. They do not require a Lookup transformation — the integer key is computed from the source date and is by definition correct if the source date is valid. However, a **validation lookup** against `DimDate` is good practice to confirm that the computed key actually exists in the calendar:

```sql
-- Validation lookup reference: just confirm the key exists
SELECT DateKey FROM Dimension.DimDate WHERE DateKey <> 0;
```

This catches dates outside the DW's calendar range (e.g., a future order date that predates a calendar extension) that would violate the FK constraint at load time.

---

## 4. The Derived Column Transformation

The **Derived Column** transformation adds new columns to the pipeline or replaces existing column values using SSIS expressions. It is used when transformation logic cannot be expressed in the source SQL — typically when it depends on values computed by earlier transformations in the same pipeline, or when type conversions require SSIS-specific syntax.

### 4.1 SSIS Expression Language

SSIS uses its own expression language — syntactically similar to C# but distinct from T-SQL. Key differences:

| Feature | T-SQL syntax | SSIS expression syntax |
|---|---|---|
| String literals | `'single quotes'` | `"double quotes"` |
| Conditional | `CASE WHEN x THEN y ELSE z END` | `x ? y : z` (ternary) |
| NULL check | `IS NULL` | `ISNULL(column)` |
| NULL substitute | `ISNULL(col, default)` | `ISNULL(col) ? default : col` |
| String concat | `col1 + col2` | `col1 + col2` (same) |
| Type cast | `CAST(x AS INT)` | `(DT_I4)x` |
| Date part | `YEAR(date)` | `YEAR(date)` (same) |

**Common SSIS data type codes:**

| Type code | SQL Server equivalent |
|---|---|
| `DT_I1` | TINYINT |
| `DT_I2` | SMALLINT |
| `DT_I4` | INT |
| `DT_I8` | BIGINT |
| `DT_R4` | REAL |
| `DT_R8` | FLOAT |
| `DT_DECIMAL` | DECIMAL/NUMERIC |
| `DT_WSTR` | NVARCHAR |
| `DT_STR` | VARCHAR |
| `DT_DBDATE` | DATE |
| `DT_DBTIMESTAMP` | DATETIME |
| `DT_BOOL` | BIT |

### 4.2 Common Derived Column Expressions

**Computing an integer date key from a DATE column:**
```
(DT_I4)((DT_WSTR,8)(DT_DBDATE)[OrderDate])
```
This casts the date to a string in YYYYMMDD format then converts to INT. Equivalent to `CAST(FORMAT(OrderDate, 'yyyyMMdd') AS INT)` in T-SQL.

**A simpler approach using YEAR, MONTH, DAY:**
```
YEAR([OrderDate]) * 10000 + MONTH([OrderDate]) * 100 + DAY([OrderDate])
```
Both produce the same integer result; the second is more readable.

**CustomerTier derivation from CreditLimit:**
```
[CreditLimit] > 10000 ? "Enterprise" : ([CreditLimit] >= 1000 ? "Standard" : "New")
```

**NULL substitution:**
```
ISNULL([ProductCode]) ? "No Code" : [ProductCode]
```

**MarginTier from CategoryName:**
```
[CategoryName] == "Accessories" || [CategoryName] == "Hydration" ||
[CategoryName] == "Navigation"  || [CategoryName] == "Safety and First Aid"
? "High"
: ([CategoryName] == "Climbing and Rappelling" || [CategoryName] == "Cooking and Food" ||
   [CategoryName] == "Apparel - Outerwear"     || [CategoryName] == "Apparel - Tops"   ||
   [CategoryName] == "Backpacks and Bags"
   ? "Medium"
   : "Low")
```

**IsWeekend flag from DayOfWeekNumber:**
```
[DayOfWeekNumber] == 1 || [DayOfWeekNumber] == 7 ? (DT_BOOL)1 : (DT_BOOL)0
```

### 4.3 When to Use Derived Column vs Source SQL

The choice between implementing a derivation in the source SQL query or in a SSIS Derived Column transformation follows a practical rule:

**Use source SQL when:**
- The derivation depends only on source table columns
- The logic is a CASE expression, calculation, or JOIN that SQL handles naturally
- The result is needed by downstream SSIS transformations (it can be referenced in Lookup join conditions)

**Use SSIS Derived Column when:**
- The derivation depends on a column produced by an earlier SSIS transformation (e.g., a surrogate key added by a Lookup)
- The derivation requires SSIS-specific type casting
- A flag or indicator needs to be set based on a Lookup match/no-match outcome

For CabotTrail ETL, most derivations are implemented in source SQL. The SSIS Derived Column transformation is used primarily for:
1. Setting `IsInvoice`, `IsPayment`, `IsCreditNote` flags based on `TransactionTypeName` (after the TransactionType lookup)
2. Type conversions that SSIS requires explicitly

---

## 5. The Conditional Split Transformation

The **Conditional Split** transformation routes rows to different outputs based on conditions. It is the primary tool for implementing data quality error handling in the Data Flow.

### 5.1 How Conditional Split Works

A Conditional Split evaluates conditions in order and routes each row to the first matching output. Rows that match no condition go to the **default output**.

```
Rows entering Conditional Split:

Condition 1: UnitPrice > 0 AND Quantity > 0 AND CustomerID IS NOT NULL
  → Output: ValidRows → continues to Lookup transformations

Condition 2: UnitPrice <= 0
  → Output: InvalidPrice → Error destination

Condition 3: Quantity <= 0
  → Output: InvalidQuantity → Error destination

Default: everything else (shouldn't happen if conditions are complete)
  → Output: UnexpectedRows → Error destination
```

### 5.2 SSIS Expression Syntax for Conditions

```
-- Valid row condition
[UnitPrice] > 0 && [Quantity] > 0 && ![ISNULL([CustomerID])]

-- Invalid price
[UnitPrice] <= 0

-- NULL CustomerID
ISNULL([CustomerID])
```

Note the SSIS logical operators: `&&` (AND), `||` (OR), `!` (NOT).

### 5.3 The Error Destination

The error output from a Conditional Split (or from a Lookup no-match output) should be directed to an **error logging table** in the DW:

```sql
-- Error log table for ETL load failures
USE CabotTrailOutdoorDW;
GO

CREATE TABLE ETL.LoadErrors
(
    ErrorID         INT             NOT NULL IDENTITY(1,1),
    PackageName     NVARCHAR(200)   NOT NULL,
    ComponentName   NVARCHAR(200)   NOT NULL,
    ErrorCode       INT             NULL,
    ErrorColumn     INT             NULL,
    ErrorDescription NVARCHAR(MAX)  NULL,
    RejectedRow     NVARCHAR(MAX)   NULL,   -- JSON or concatenated key values
    LoadDate        DATETIME2       NOT NULL DEFAULT SYSDATETIME(),
    CONSTRAINT PK_ETL_LoadErrors PRIMARY KEY (ErrorID)
);
GO
```

In SSIS, the OLE DB Destination pointed at `ETL.LoadErrors` receives rejected rows. Package-level variables (`@PackageName`, `@ComponentName`) populated by SSIS system variables provide context for each error record.

### 5.4 Implementing the DQ Strategy in SSIS

Recall the three error handling strategies from Chapter 3. Here is how each maps to SSIS components:

**Fail fast:** Use a `Fail component` setting on the Lookup or add a Conditional Split that routes invalid rows to a **Script Task** that throws an exception — terminating the entire package.

**Reject and continue:** Use Conditional Split or Lookup no-match output → **OLE DB Destination** pointing at `ETL.LoadErrors`. The main flow continues with valid rows.

**Substitute and continue:** Use a **Derived Column** transformation to replace the invalid value with a default before writing to the destination. Add a `Flag_Substituted` column set to TRUE for rows where substitution occurred, and log these to an audit table.

---

## 6. The OLE DB Destination

The **OLE DB Destination** writes rows from the pipeline into a SQL Server table. It is the final component in most Data Flow pipelines.

### 6.1 Configuration

**Connection:** The DW connection manager.

**Table:** The target dimension or fact table. Always select the table explicitly — never use the auto-create option in production, as it generates tables without constraints, correct data types, or indexes.

**Column mapping:** Map each pipeline column to its target table column. SSIS attempts automatic mapping by name — always verify this manually, especially when source and target column names differ.

### 6.2 Fast Load vs Standard Load

OLE DB Destination offers two write modes:

**Table or view — fast load** (recommended): Uses SQL Server's bulk insert mechanism. Significantly faster than row-by-row insertion for large datasets. Supports options:
- `Keep identity` — do not generate surrogate keys; use values from the pipeline (use when loading from a DW to a data mart where surrogate keys are carried over)
- `Keep nulls` — preserve explicit NULLs rather than substituting column defaults
- `Table lock` — acquire a table-level lock for the duration of the load (faster but blocks concurrent access)
- `Check constraints` — enforce CHECK constraints during load (slight performance cost but catches violations)
- `Rows per batch` — number of rows per bulk insert batch (default 0 = all rows in one batch)
- `Maximum insert commit size` — number of rows per commit (smaller = more frequent commits = better recoverability)

**Table or view — standard load**: Row-by-row insertion. Much slower. Use only when fast load is not appropriate (e.g., when inserting into tables with triggers that must fire per row).

### 6.3 Keep Identity: Loading Data Marts from the DW

When loading a data mart (e.g., `CabotTrailOutdoorsSales.dim.Customer`) from the DW (`CabotTrailOutdoorDW.Dimension.DimCustomer`), the surrogate keys from the DW must be preserved exactly. The data mart FK relationships depend on the same surrogate key values existing in both databases.

**In the OLE DB Destination for data mart loads:**
- Enable `Keep identity` — this tells SQL Server to use the `CustomerKey` value from the pipeline rather than generating a new IDENTITY value
- The target table's IDENTITY column must have `IDENTITY_INSERT ON` active during the load

In T-SQL, this is done explicitly:

```sql
SET IDENTITY_INSERT dim.Customer ON;

INSERT INTO dim.Customer (CustomerKey, CustomerID, CustomerName, ...)
SELECT CustomerKey, CustomerID, CustomerName, ...
FROM   CabotTrailOutdoorDW.Dimension.DimCustomer;

SET IDENTITY_INSERT dim.Customer OFF;
```

In SSIS, enabling `Keep identity` on the OLE DB Destination handles this automatically.

---

## 7. Building a Dimension Load Package

With each individual component understood, we can build a complete dimension load package. This section walks through `Load_DimProduct.dtsx` — the most complex CabotTrail dimension because it requires two lookup transformations.

### 7.1 Package Design

```
Control Flow:
┌─────────────────────────────────────────────────────────┐
│  [Execute SQL: Truncate DimProduct]                     │
│          ↓ Success                                      │
│  [Data Flow: Load DimProduct]                           │
│          ↓ Success                                      │
│  [Execute SQL: Record reconciliation test]              │
│          ↓ Failure                                      │
│  [Execute SQL: Log error to ETL.LoadErrors]             │
└─────────────────────────────────────────────────────────┘

Data Flow:
┌─────────────────────────────────────────────────────────┐
│  [OLE DB Source: Products extract query]                │
│          ↓                                              │
│  [Lookup: ProductCategoryID → ProductCategoryKey]       │
│          ├── Match → continues                          │
│          └── No Match → [Error OLE DB Destination]      │
│  [Lookup: SupplierID → SupplierKey]                     │
│          ├── Match → continues                          │
│          └── No Match → [Error OLE DB Destination]      │
│  [OLE DB Destination: DimProduct]                       │
└─────────────────────────────────────────────────────────┘
```

### 7.2 Execute SQL Task: Truncate

```sql
-- Truncate statement in Execute SQL Task
TRUNCATE TABLE Dimension.DimProduct;
```

> **Why truncate instead of delete?** `TRUNCATE TABLE` is a minimally logged operation — it deallocates all data pages without logging individual row deletions. For a table with 142 rows, the difference is negligible. For fact tables with millions of rows, truncate is orders of magnitude faster than DELETE.
>
> **Constraint note:** `TRUNCATE TABLE` is blocked when there are active FK references from other tables. This is why fact tables must be truncated before their dimensions in a full reload. The reverse (truncating a dimension while its fact table still has rows) will fail with a constraint violation.

### 7.3 OLE DB Source: Products Extract Query

```sql
-- Products extract query (in OLE DB Source > SQL Command)
SELECT
    p.ProductID,
    p.ProductCode,
    p.ProductName,
    pca.ProductCategoryID,      -- Used in Lookup 1 below
    c.ColorName,
    pt.PackageTypeName,
    p.UnitPrice,
    p.RecommendedRetailPrice,
    p.TypicalWeightPerUnit,
    p.IsDiscontinued,
    p.SupplierID                -- Used in Lookup 2 below
FROM    Inventory.Products p
LEFT  JOIN Inventory.Colors c
    ON c.ColorID = p.ColorID
INNER JOIN Inventory.PackageTypes pt
    ON pt.PackageTypeID = p.PackageTypeID
INNER JOIN (
    -- Resolve M:M to primary category
    SELECT  ProductID,
            MIN(ProductCategoryID) AS ProductCategoryID
    FROM    Inventory.ProductCategoryAssignments
    GROUP BY ProductID
) pca ON pca.ProductID = p.ProductID
WHERE   p.ProductID <> 0;
```

### 7.4 Lookup 1: ProductCategoryID → ProductCategoryKey

**Reference query:**
```sql
SELECT  ProductCategoryID, ProductCategoryKey
FROM    CabotTrailOutdoorDW.Dimension.DimProductCategory
WHERE   ProductCategoryID <> 0;
```

**Join column:** `ProductCategoryID` (pipeline) → `ProductCategoryID` (lookup)
**Output column:** `ProductCategoryKey` → add to pipeline
**No match behaviour:** Redirect rows to no match output → Error OLE DB Destination

> **Prerequisite:** `DimProductCategory` must be loaded before `DimProduct`. If this lookup runs against an empty `DimProductCategory`, every row will be a no-match and the entire load will produce 0 rows in `DimProduct`. This is the load order dependency in action.

### 7.5 Lookup 2: SupplierID → SupplierKey

**Reference query:**
```sql
SELECT  SupplierID, SupplierKey
FROM    CabotTrailOutdoorDW.Dimension.DimSupplier
WHERE   SupplierID <> 0;
```

**Join column:** `SupplierID` (pipeline) → `SupplierID` (lookup)
**Output column:** `SupplierKey` → add to pipeline
**No match behaviour:** Redirect rows to no match output → Error OLE DB Destination

### 7.6 OLE DB Destination: DimProduct

**Connection:** CabotTrailOutdoorDW
**Table:** `[Dimension].[DimProduct]`
**Keep identity:** OFF — `ProductKey` is an IDENTITY column generated by the DW

**Column mapping (key mappings to verify):**

| Pipeline column | Target column |
|---|---|
| `ProductCategoryKey` | `ProductCategoryKey` |
| `SupplierKey` | `SupplierKey` |
| `ProductID` | `ProductID` |
| `ProductName` | `ProductName` |
| `ColorName` | `ColorName` |
| `PackageTypeName` | `PackageTypeName` |

### 7.7 Execute SQL Task: Reconciliation

After the Data Flow completes, the Execute SQL Task runs a reconciliation check and logs the result:

```sql
-- Reconciliation test for DimProduct
INSERT INTO ETL.TestResults
    (TestName, TestCategory, TargetTable, ExpectedValue, ActualValue, Passed)
SELECT
    'DimProduct Row Count vs Source'    AS TestName,
    'Reconciliation'                    AS TestCategory,
    'Dimension.DimProduct'              AS TargetTable,
    CAST(src.SourceCount AS NVARCHAR)   AS ExpectedValue,
    CAST(dw.DWCount AS NVARCHAR)        AS ActualValue,
    CASE WHEN src.SourceCount = dw.DWCount THEN 1 ELSE 0 END AS Passed
FROM
    (SELECT COUNT(*) AS SourceCount
     FROM CabotTrailOutdoor.Inventory.Products
     WHERE ProductID <> 0) src,
    (SELECT COUNT(*) AS DWCount
     FROM Dimension.DimProduct
     WHERE ProductID <> 0) dw;
```

---

## 8. Building a Fact Table Load Package

Fact table load packages are structured identically to dimension packages — the complexity lies in the number of lookup transformations required and the precision of the measure derivations.

### 8.1 Package Design: Load_FactSales.dtsx

```
Control Flow:
[Execute SQL: Truncate Fact.FactSales]
        ↓ Success
[Data Flow: Load FactSales]
        ↓ Success
[Execute SQL: Record reconciliation tests (row count + revenue)]
        ↓ Failure
[Execute SQL: Log error]

Data Flow:
[OLE DB Source: FactSales extract query]
        ↓
[Lookup: CustomerID → CustomerKey]
        ├── Match ↓
        └── No Match → [Error Destination]
[Lookup: ProductID → ProductKey]
        ├── Match ↓
        └── No Match → [Error Destination]
[Lookup: EmployeeID → EmployeeKey]
        ├── Match ↓
        └── No Match → [Error Destination]
[Lookup: CityID → GeographyKey]
        ├── Match ↓
        └── No Match → [Error Destination]
[Lookup: DeliveryMethodID → DeliveryMethodKey]
        ├── Match ↓
        └── No Match → [Error Destination]
[Lookup: OrderDateKey → validate in DimDate]
        ├── Match ↓
        └── No Match → [Error Destination]
[Lookup: InvoiceDateKey → validate in DimDate]
        ├── Match ↓
        └── No Match → [Error Destination]
[Lookup: DueDateKey → validate in DimDate]
        ├── Match ↓
        └── No Match → [Error Destination]
[OLE DB Destination: Fact.FactSales]
```

### 8.2 Extract Query

The complete extract query developed in Chapter 3 is entered in the OLE DB Source. All measure derivations (UnitCost, GrossProfit, GrossProfitMarginPct) are computed in the source SQL. The output columns include natural keys (`CustomerID`, `ProductID`, etc.) that will be replaced by surrogate keys in the subsequent Lookup transformations.

### 8.3 Five Surrogate Key Lookups

Each of the five dimension FKs requires a Lookup transformation. The pattern is identical for all five:

| Lookup | Reference query | Join col | Output col |
|---|---|---|---|
| Customer | `SELECT CustomerID, CustomerKey FROM DimCustomer WHERE CustomerID <> 0` | `CustomerID` | `CustomerKey` |
| Product | `SELECT ProductID, ProductKey FROM DimProduct WHERE ProductID <> 0` | `ProductID` | `ProductKey` |
| Employee | `SELECT EmployeeID, EmployeeKey FROM DimEmployee WHERE EmployeeID <> 0` | `EmployeeID` | `EmployeeKey` |
| Geography | `SELECT CityID, GeographyKey FROM DimGeography WHERE CityID <> 0` | `CityID` | `GeographyKey` |
| DeliveryMethod | `SELECT DeliveryMethodID, DeliveryMethodKey FROM DimDeliveryMethod WHERE DeliveryMethodID <> 0` | `DeliveryMethodID` | `DeliveryMethodKey` |

### 8.4 Three Date Key Validations

The three date keys (`OrderDateKey`, `InvoiceDateKey`, `DueDateKey`) were computed in the source SQL as YYYYMMDD integers. Validation lookups confirm they exist in `DimDate`:

```sql
-- Reference query for all three date validation lookups
SELECT DateKey FROM Dimension.DimDate WHERE DateKey <> 0;
```

No output column is added — the only purpose is to confirm the key exists. A no-match means a date outside the calendar range, which would violate the FK constraint at load time.

### 8.5 OLE DB Destination: Fact.FactSales

At the destination, after all eight lookups, the pipeline contains:
- The three YYYYMMDD date keys (computed at source)
- The five dimension surrogate keys (added by lookups)
- All degenerate dimensions (InvoiceID, OrderID, OrderLineID)
- All measures (OrderedQuantity, PickedQuantity, UnitPrice, TaxRate, LineTotal, TaxAmount, LineTotalIncludingTax, UnitCost, GrossProfit, GrossProfitMarginPct)

The destination maps each pipeline column to its target column. `SalesKey` is the IDENTITY PK — it is not in the pipeline and not mapped. SQL Server generates it automatically on insert.

### 8.6 Reconciliation for FactSales

Two reconciliation tests run after the fact load:

```sql
-- Test 1: Row count
INSERT INTO ETL.TestResults
    (TestName, TestCategory, TargetTable, ExpectedValue, ActualValue, Passed)
SELECT
    'FactSales Row Count vs Source',
    'Reconciliation', 'Fact.FactSales',
    CAST(src.c AS NVARCHAR),
    CAST(dw.c  AS NVARCHAR),
    CASE WHEN src.c = dw.c THEN 1 ELSE 0 END
FROM
    (SELECT COUNT(*) AS c
     FROM CabotTrailOutdoor.Sales.InvoiceLines il
     INNER JOIN CabotTrailOutdoor.Sales.Invoices i ON i.InvoiceID = il.InvoiceID
     INNER JOIN CabotTrailOutdoor.Sales.Orders o   ON o.OrderID   = i.OrderID
     WHERE YEAR(o.OrderDate) < 2026) src,
    (SELECT COUNT(*) AS c FROM Fact.FactSales) dw;

-- Test 2: Revenue reconciliation
INSERT INTO ETL.TestResults
    (TestName, TestCategory, TargetTable, ExpectedValue, ActualValue, Passed)
SELECT
    'FactSales Revenue vs Source',
    'Reconciliation', 'Fact.FactSales',
    CAST(src.rev AS NVARCHAR),
    CAST(dw.rev  AS NVARCHAR),
    CASE WHEN ABS(src.rev - dw.rev) < 0.01 THEN 1 ELSE 0 END
FROM
    (SELECT ROUND(SUM(il.LineTotal), 2) AS rev
     FROM CabotTrailOutdoor.Sales.InvoiceLines il
     INNER JOIN CabotTrailOutdoor.Sales.Invoices i ON i.InvoiceID = il.InvoiceID
     INNER JOIN CabotTrailOutdoor.Sales.Orders o   ON o.OrderID   = i.OrderID
     WHERE YEAR(o.OrderDate) < 2026) src,
    (SELECT ROUND(SUM(LineTotal), 2) AS rev FROM Fact.FactSales) dw;
```

> **The 0.01 tolerance** in the revenue test accounts for floating-point rounding differences between the source and DW computations. Both are DECIMAL columns, so the tolerance should be zero — but 0.01 is a conservative guard against minor platform-level precision differences.

---

## 9. Reconciliation and Verification

Reconciliation is not optional. It is the mechanism that distinguishes a trusted ETL system from one that might be loading incorrect data silently.

### 9.1 The Reconciliation Framework

A complete reconciliation framework checks three things after each load:

**Row count reconciliation.** The number of rows in the target must match the number of rows in the source (after applying the same filters). A discrepancy indicates either dropped rows (lookup failures, filter errors) or duplicated rows (JOIN fan-out).

```sql
-- Template: row count reconciliation
SELECT
    'Target rows'   AS Metric,
    COUNT(*)        AS Value
FROM    [target_table]
UNION ALL
SELECT
    'Source rows',
    COUNT(*)
FROM    [source_query_with_same_filters];
```

**Aggregate reconciliation.** At least one key measure must be summed in both source and target and compared. The most meaningful measure for `FactSales` is total revenue (`LineTotal`).

```sql
-- Template: aggregate reconciliation
SELECT
    'Target revenue'    AS Metric,
    SUM(LineTotal)      AS Value
FROM    Fact.FactSales
UNION ALL
SELECT
    'Source revenue',
    SUM(il.LineTotal)
FROM    Sales.InvoiceLines il
INNER JOIN Sales.Invoices i ON i.InvoiceID = il.InvoiceID
INNER JOIN Sales.Orders o   ON o.OrderID   = i.OrderID
WHERE   YEAR(o.OrderDate) < 2026;
```

**Orphan check.** After every fact load, verify that no fact row has a FK value that does not exist in the referenced dimension. A passing orphan check confirms referential integrity:

```sql
-- Orphan check: FactSales
SELECT  COUNT(*) AS OrphanRows
FROM    Fact.FactSales fs
WHERE   NOT EXISTS (SELECT 1 FROM Dimension.DimDate d
                    WHERE d.DateKey = fs.OrderDateKey)
OR      NOT EXISTS (SELECT 1 FROM Dimension.DimCustomer c
                    WHERE c.CustomerKey = fs.CustomerKey)
OR      NOT EXISTS (SELECT 1 FROM Dimension.DimProduct p
                    WHERE p.ProductKey = fs.ProductKey)
OR      NOT EXISTS (SELECT 1 FROM Dimension.DimEmployee e
                    WHERE e.EmployeeKey = fs.EmployeeKey)
OR      NOT EXISTS (SELECT 1 FROM Dimension.DimGeography g
                    WHERE g.GeographyKey = fs.GeographyKey)
OR      NOT EXISTS (SELECT 1 FROM Dimension.DimDeliveryMethod dm
                    WHERE dm.DeliveryMethodKey = fs.DeliveryMethodKey);
-- Expected: 0
```

### 9.2 Diagnosing Reconciliation Failures

When row count reconciliation fails — the DW has fewer rows than the source — the first diagnostic step is to check the error tables:

```sql
-- Check for lookup failures during the load
SELECT  PackageName, ComponentName, ErrorDescription, COUNT(*) AS RejectedRows
FROM    ETL.LoadErrors
WHERE   CAST(LoadDate AS DATE) = CAST(GETDATE() AS DATE)
GROUP BY PackageName, ComponentName, ErrorDescription
ORDER BY RejectedRows DESC;
```

The most common cause of row count discrepancies is a surrogate key lookup failure — a source row referenced a natural key that had no matching surrogate in the dimension. The error table will identify exactly which component rejected the rows and how many.

When revenue reconciliation fails but row counts match, the issue is a transformation defect — the derived `LineTotal` or measure computation is producing different values than the source. Compare individual rows between source and target to isolate the discrepancy:

```sql
-- Find revenue discrepancies at the invoice line level
SELECT
    dw.InvoiceID,
    dw.OrderLineID,
    dw.LineTotal        AS DW_LineTotal,
    src.LineTotal       AS Src_LineTotal,
    dw.LineTotal - src.LineTotal AS Difference
FROM    Fact.FactSales dw
INNER JOIN CabotTrailOutdoor.Sales.InvoiceLines src
    ON src.InvoiceID    = dw.InvoiceID
    AND src.OrderLineID = dw.OrderLineID
WHERE   ABS(dw.LineTotal - src.LineTotal) > 0.01
ORDER BY ABS(dw.LineTotal - src.LineTotal) DESC;
```

---

## 10. The Master Package and Load Order

Individual packages for each dimension and fact table are the building blocks. The **master package** orchestrates all of them in the correct dependency order.

### 10.1 The Execute Package Task

The Control Flow task for calling a child package is the **Execute Package Task**. It can reference packages in:
- **The file system** — path to a `.dtsx` file (development only)
- **The SSIS Catalog** — a deployed package in `SSISDB` (production)

For production deployment, always use the SSIS Catalog reference. This decouples the master package from file system paths and supports environment-specific configuration.

### 10.2 Load Order: Dependency Chain

The complete load order for CabotTrail DW implements the five-tier dependency chain from Chapter 3:

```
Tier 1 — No dependencies (can run in parallel):
    Load_DimDate.dtsx
    Load_DimDeliveryMethod.dtsx
    Load_DimTransactionType.dtsx
    Load_DimPaymentMethod.dtsx
    Load_DimProductCategory.dtsx

Tier 2 — Depends on no other DW dims:
    Load_DimGeography.dtsx

Tier 3 — Depends on DimGeography:
    Load_DimCustomer.dtsx
    Load_DimSupplier.dtsx
    Load_DimEmployee.dtsx

Tier 4 — Depends on DimProductCategory + DimSupplier:
    Load_DimProduct.dtsx

Tier 5 — Depends on all dimensions:
    Load_FactSales.dtsx
    Load_FactPurchasing.dtsx
    Load_FactReturns.dtsx
    Load_FactInventory.dtsx
    Load_FactCustomerTransactions.dtsx
    Load_FactSupplierTransactions.dtsx
```

### 10.3 Master Package Control Flow

```
Sequence Container: Tier 1 Dimensions
    [Load_DimDate]  [Load_DimDeliveryMethod]  [Load_DimTransactionType]
    [Load_DimPaymentMethod]  [Load_DimProductCategory]
    (all run in parallel within the container)
        ↓ Container Success
Sequence Container: Tier 2 Dimensions
    [Load_DimGeography]
        ↓ Success
Sequence Container: Tier 3 Dimensions
    [Load_DimCustomer]  [Load_DimSupplier]  [Load_DimEmployee]
    (all run in parallel)
        ↓ Container Success
[Load_DimProduct]
        ↓ Success
Sequence Container: Fact Tables
    [Load_FactSales]  [Load_FactPurchasing]  [Load_FactReturns]
    [Load_FactInventory]  [Load_FactCustomerTransactions]  [Load_FactSupplierTransactions]
    (all run in parallel)
        ↓ Container Success / Failure
[Execute SQL: Final reconciliation summary]
```

### 10.4 Parallelism in the Control Flow

Tasks within a Sequence Container with no precedence constraints between them run in parallel. SSIS manages thread allocation automatically. Parallel execution of independent dimension loads (Tier 1 and Tier 3) significantly reduces total load time.

**Important:** Tasks in parallel must be truly independent. Parallelizing dimension loads that depend on each other (e.g., trying to load `DimProduct` in parallel with `DimProductCategory`) will produce intermittent failures depending on which loads faster in any given run.

### 10.5 SSIS Variables for Package Configuration

Rather than hardcoding connection strings and table names in each package, use **SSIS variables** — named values defined at the package or project level that can be changed without modifying package logic.

Project-level variables defined once apply to all packages in the project:

| Variable | Type | Value |
|---|---|---|
| `@SourceServer` | String | `localhost` (or server name) |
| `@SourceDatabase` | String | `CabotTrailOutdoor` |
| `@TargetDatabase` | String | `CabotTrailOutdoorDW` |
| `@LoadDate` | DateTime | Set to GETDATE() at runtime |
| `@PackageName` | String | Set from system variable `@[System::PackageName]` |

Connection Managers can reference variables using expressions, making each package automatically adapt to the configured server and database names.

---

## 11. Chapter Summary

- SSIS separates orchestration (**Control Flow**) from data movement (**Data Flow**). The Control Flow determines what runs and in what order. The Data Flow processes rows through a streaming transformation pipeline.

- The **OLE DB Source** reads rows using a SQL query. The extract query from the S2T mapping is entered here directly. Transformations that can be expressed in SQL should be implemented in the source query rather than in SSIS transformations.

- The **Lookup transformation** resolves natural keys to surrogate keys. It is the central operation of dimensional ETL. Every fact FK requires a corresponding Lookup. No-match rows must be redirected to error outputs — never silently dropped or loaded as NULL FKs.

- The **Derived Column transformation** adds computed columns to the pipeline using SSIS expressions. Use it for derivations that depend on pipeline-produced values or that require SSIS-specific type handling.

- The **Conditional Split transformation** routes rows to different outputs based on conditions. It is the primary tool for data quality error handling — valid rows continue to the destination; invalid rows go to error tables.

- The **OLE DB Destination** writes rows to SQL Server. Fast load mode uses bulk insert for performance. `Keep identity` must be enabled when preserving surrogate keys across database boundaries (DW to data mart loads).

- **Reconciliation** — row count, aggregate, and orphan checks — must run after every load. Discrepancies indicate defects that must be diagnosed and resolved before the ETL is considered correct.

- The **master package** orchestrates all dimension and fact loads in dependency order using Execute Package Tasks. Dimensions at the same tier can be parallelized within Sequence Containers.

---

## 12. Review Questions

1. Explain why a Lookup transformation with "Fail component" behaviour is never appropriate for a production ETL package. What should be used instead, and what happens to the rejected rows?

2. The `DimProduct` load package runs successfully but produces 0 rows in the target. The OLE DB Source returned 142 rows. What is the most likely cause, and how would you diagnose it?

3. You need to load `DimSupplier`. Its extract query JOINs `Purchasing.Suppliers` to `Application.Cities`, `Application.StateProvinces`, and `Application.Countries`. In the SSIS Data Flow, should these JOINs be implemented in the OLE DB Source SQL query or in SSIS Lookup transformations? Justify your answer.

4. After loading `Fact.FactSales`, the row count reconciliation passes (16,359 rows in both source and DW) but the revenue reconciliation fails — the DW total is $42 lower than the source total. List three possible causes and describe the diagnostic query you would run for each.

5. A new column `PromotionCode` is being added to `Fact.FactSales`. It comes from a new OLTP table `Marketing.PromoCodes` which has `OrderLineID` and `PromotionCode`. About 30% of order lines have a promo code; the other 70% do not. Describe the complete implementation: the join type in the source query, the NULL handling, and any SSIS components needed beyond the source and destination.

6. The Tier 3 dimensions `DimCustomer`, `DimSupplier`, and `DimEmployee` are currently loaded sequentially. A colleague suggests loading them in parallel to reduce load time. Is this safe? Under what conditions could parallel loading of these three dimensions cause problems?

7. In the master package, what happens if `Load_DimProduct` fails? Describe the behaviour of the downstream fact loads and explain why this is the correct behaviour.

8. Explain the difference between `TRUNCATE TABLE` and `DELETE FROM` in the context of an ETL load. When would you use DELETE instead of TRUNCATE, and why?

---

## 🔍 Deeper Dive

### Going Further with ETL Implementation

#### SSIS Buffer Tuning for Performance

The SSIS Data Flow buffer architecture described in section 1.3 is configurable. For high-volume loads, tuning buffer parameters can significantly improve throughput:

**`DefaultBufferMaxRows`** — maximum number of rows in a single buffer (default: 10,000). Increasing this for wide rows (many columns) reduces the number of buffer handoffs in the pipeline.

**`DefaultBufferSize`** — maximum size of a single buffer in bytes (default: 10 MB). The actual buffer size is the minimum of `DefaultBufferMaxRows × row_size` and `DefaultBufferSize`. For narrow rows, the row limit is binding; for wide rows, the byte limit is binding.

**`EngineThreads`** — number of threads available to the Data Flow engine (default: 10). Increasing this allows more parallel processing within a single Data Flow.

These parameters are set at the Data Flow Task level (not the package level). Microsoft's documentation provides detailed guidance:
[SSIS — Data Flow Performance Features](https://learn.microsoft.com/en-us/sql/integration-services/data-flow/data-flow-performance-features)

#### The SSIS Catalog: Deployment and Monitoring

The **SSIS Catalog** (`SSISDB`) is the production deployment and monitoring platform for SSIS packages. It provides:

- **Package deployment** — packages are deployed as projects to the catalog, versioned, and executed from there
- **Environment configuration** — environment variables (server names, passwords, paths) are stored separately from package logic, allowing the same package to run in DEV, Test, and Production without modification
- **Execution logging** — every package execution is logged with start/end times, parameter values, and messages
- **Operation messages** — error messages, warnings, and informational messages are stored per execution, accessible via SSMS or T-SQL views

Key catalog views for monitoring:

```sql
-- Recent executions and their status
SELECT  execution_id, package_name, project_name,
        start_time, end_time, status,
        DATEDIFF(SECOND, start_time, end_time) AS DurationSeconds
FROM    SSISDB.catalog.executions
ORDER BY start_time DESC;

-- Error messages from a failed execution
SELECT  message_time, message_type, message
FROM    SSISDB.catalog.operation_messages
WHERE   operation_id = [execution_id_from_above]
AND     message_type IN (120, 130)  -- 120=Error, 130=Warning
ORDER BY message_time;

-- Package performance: rows per second for each data flow component
SELECT  execution_id, package_name, task_name,
        rows_read, rows_sent
FROM    SSISDB.catalog.execution_data_statistics
WHERE   execution_id = [execution_id_from_above]
ORDER BY task_name;
```

Microsoft documentation for the SSIS Catalog:
[SSIS Catalog (SSISDB)](https://learn.microsoft.com/en-us/sql/integration-services/catalog/ssis-catalog)

#### The Script Component: Extending the Data Flow

When built-in SSIS transformations cannot meet a requirement, the **Script Component** allows custom C# or VB.NET code to execute within the Data Flow pipeline. Use cases include:

- Parsing complex or non-standard file formats
- Calling web services to enrich pipeline rows
- Implementing custom fuzzy matching logic for deduplication
- Complex string manipulation that SSIS expressions cannot handle

The Script Component can act as a Source, Transformation, or Destination. It has full access to ADO.NET and .NET Framework libraries, making it enormously powerful — but also complex to maintain.

> **Design principle:** Use the Script Component as a last resort. Every Script Component requires a developer who understands both SSIS and C#/.NET to maintain it. If the same transformation can be achieved in a source SQL query or a Derived Column expression, prefer those.

Microsoft documentation:
[Script Component — SSIS](https://learn.microsoft.com/en-us/sql/integration-services/data-flow/transformations/script-component)

#### T-SQL as an ETL Alternative

SSIS is not the only way to implement ETL against SQL Server. T-SQL stored procedures implementing `TRUNCATE TABLE` + `INSERT INTO ... SELECT` (the pattern from DBAS 5010) are a legitimate alternative, particularly for:

- Simple dimension loads with no complex transformations
- Environments where SSIS licensing or tooling is not available
- ETL processes that run within a single SQL Server instance (no cross-server movement)
- Rapid prototyping and development

The trade-offs compared to SSIS:

| Factor | SSIS | T-SQL |
|---|---|---|
| Performance | ✅ Streaming buffers; parallel within pipeline | ✅ Bulk insert; set-based operations |
| Error handling | ✅ Row-level redirect to error output | ⚠️ Entire statement fails; no row-level routing without TRY/CATCH |
| Monitoring | ✅ SSIS Catalog with full execution history | ⚠️ SQL Agent job history; manual logging required |
| Cross-source | ✅ Any OLEDB source; files, APIs | ⚠️ Linked servers required for cross-instance |
| Maintainability | ✅ Visual representation of pipeline | ✅ Code-readable, version-controllable |
| Deployment | ⚠️ Requires SSIS infrastructure | ✅ Standard SQL; runs anywhere SQL Server runs |

For the CabotTrail environment, both approaches are valid. The T-SQL ETL scripts developed in DBAS 5010 and DBAS 2010 are functionally equivalent to the SSIS packages in this chapter. Production environments typically use SSIS for its monitoring, error handling, and scheduling integration capabilities.

#### Change Data Capture Integration with SSIS

For incremental ETL (loading only rows changed since the last load), SSIS has a dedicated **CDC Source** component that reads directly from SQL Server's Change Data Capture tables:

```sql
-- CDC tables created by SQL Server when CDC is enabled
-- Accessible via Change Data Capture functions
SELECT  *
FROM    cdc.fn_cdc_get_all_changes_Sales_Orders(
            @from_lsn,      -- Log sequence number from last run
            @to_lsn,        -- Current log sequence number
            'all'           -- Include inserts, updates, deletes
        );
```

The SSIS CDC Source component wraps this function and provides:
- Automatic LSN (Log Sequence Number) management
- Row classification: `INSERT`, `UPDATE BEFORE`, `UPDATE AFTER`, `DELETE`
- Integration with the CDC Control Task for managing the extraction window

For the CabotTrail full-reload pattern, CDC is not needed. For incremental ETL in production environments with high data volumes, CDC integration with SSIS is the standard approach.

Microsoft documentation:
[CDC Source — SSIS](https://learn.microsoft.com/en-us/sql/integration-services/data-flow/cdc-source)
[Using CDC with SSIS](https://learn.microsoft.com/en-us/sql/integration-services/change-data-capture/change-data-capture-ssis)

---

### Industry Perspectives

#### Kimball on the ETL System

Kimball's *The Data Warehouse ETL Toolkit* (co-authored with Joe Caserta) dedicates an entire chapter to what they call the "ETL System Architecture." Their key insight is that ETL is not a collection of scripts — it is a system with distinct subsystems:

- **Extract subsystem** — manages connection to sources, handles extraction timing and volume
- **Cleanse and conform subsystem** — implements data quality rules and conforming logic
- **Deliver subsystem** — manages the loading of dimensions and facts in the correct order
- **Manage subsystem** — handles scheduling, monitoring, error recovery, and metadata

Thinking of ETL as a system rather than a collection of packages produces better-organized, more maintainable solutions. The SSIS master package pattern in this chapter is a practical implementation of Kimball's "deliver subsystem."

Kimball, R., & Caserta, J. (2004). *The Data Warehouse ETL Toolkit*. Wiley.

#### Microsoft on SSIS Best Practices

Microsoft's SSIS documentation includes a best practices guide that covers package design, performance, and maintainability:
[Integration Services Best Practices](https://learn.microsoft.com/en-us/sql/integration-services/integration-services-best-practices)

Key recommendations aligned with this chapter:
- Use parameterized queries where possible to enable plan reuse
- Set `ValidateExternalMetadata = False` in development to avoid validation errors on disconnected machines
- Use project-level connection managers (not package-level) to share connections across packages
- Name all components descriptively — default names like "OLE DB Source" and "Lookup" make debugging impossible

---

### References and Further Reading

1. Kimball, R., & Caserta, J. (2004). *The Data Warehouse ETL Toolkit*. Wiley. — The definitive reference for ETL system design. Chapters 4–7 cover transformation patterns, surrogate key management, and fact table loading in detail.

2. Microsoft. (2024). *Integration Services Data Flow*. [https://learn.microsoft.com/en-us/sql/integration-services/data-flow/integration-services-data-flow](https://learn.microsoft.com/en-us/sql/integration-services/data-flow/integration-services-data-flow)

3. Microsoft. (2024). *Lookup Transformation*. [https://learn.microsoft.com/en-us/sql/integration-services/data-flow/transformations/lookup-transformation](https://learn.microsoft.com/en-us/sql/integration-services/data-flow/transformations/lookup-transformation)

4. Microsoft. (2024). *Derived Column Transformation*. [https://learn.microsoft.com/en-us/sql/integration-services/data-flow/transformations/derived-column-transformation](https://learn.microsoft.com/en-us/sql/integration-services/data-flow/transformations/derived-column-transformation)

5. Microsoft. (2024). *Conditional Split Transformation*. [https://learn.microsoft.com/en-us/sql/integration-services/data-flow/transformations/conditional-split-transformation](https://learn.microsoft.com/en-us/sql/integration-services/data-flow/transformations/conditional-split-transformation)

6. Microsoft. (2024). *OLE DB Destination*. [https://learn.microsoft.com/en-us/sql/integration-services/data-flow/ole-db-destination](https://learn.microsoft.com/en-us/sql/integration-services/data-flow/ole-db-destination)

7. Microsoft. (2024). *Data Flow Performance Features*. [https://learn.microsoft.com/en-us/sql/integration-services/data-flow/data-flow-performance-features](https://learn.microsoft.com/en-us/sql/integration-services/data-flow/data-flow-performance-features)

8. Microsoft. (2024). *SSIS Catalog*. [https://learn.microsoft.com/en-us/sql/integration-services/catalog/ssis-catalog](https://learn.microsoft.com/en-us/sql/integration-services/catalog/ssis-catalog)

9. Microsoft. (2024). *Integration Services Best Practices*. [https://learn.microsoft.com/en-us/sql/integration-services/integration-services-best-practices](https://learn.microsoft.com/en-us/sql/integration-services/integration-services-best-practices)

10. Microsoft. (2024). *CDC Source — SSIS*. [https://learn.microsoft.com/en-us/sql/integration-services/data-flow/cdc-source](https://learn.microsoft.com/en-us/sql/integration-services/data-flow/cdc-source)

11. Knight, B., Veerman, E., Davis, J., Dickinson, B., & Moss, J. (2012). *Professional Microsoft SQL Server 2012 Integration Services*. Wrox. — Comprehensive practical reference for SSIS development; covers all components and patterns described in this chapter with extensive worked examples.

---

*Previous chapter: [Chapter 3 — Gap Analysis and Source-to-Target Mapping](../chapter-03-gap-analysis-s2t-mapping/README.md)*

*Next chapter: [Chapter 5 — Testing, Data Quality, and Governance](../chapter-05-testing-quality-governance/README.md)*

---

> **ETL for Business Intelligence** | © Patrick Dolinger, NSCC Institute of Technology
> [CC BY 4.0](https://creativecommons.org/licenses/by/4.0/) — Share and adapt freely with attribution
