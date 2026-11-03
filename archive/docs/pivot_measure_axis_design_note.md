---
title: Pivot Library Design Note - Measures As Dimension Axis
---

# Pivot Library Design Note: Measures As Dimension Axis

## Objective

Add a generic pivot-library capability that allows selected value measures to be displayed as a row or column dimension, instead of always being rendered only as separate value columns.

This is needed for Explain Greeks report layouts where fields such as `Delta Explain`, `Gamma Explain`, `Theta Explain`, and `Increment` are currently physical numeric columns, but business users sometimes want those same measures to appear as lines in the report.

## Current Data Shape

The prepared Explain Greeks data is wide. Example:

```text
Category | Asset Class  | Curve/Asset | Portfolio | Delta Explain | Gamma Explain | Theta Explain
Equity   | EquityModels | EQTY ABC    | Book      | 100           | 25            | 5
Equity   | EquityModels | EQTY ABC    | Hedge     | -80           | -10           | 2
```

Current pivot behavior treats `Delta Explain`, `Gamma Explain`, and `Theta Explain` as value fields:

```text
Rows:
  Category
  Asset Class
  Curve/Asset

Columns:
  Portfolio

Values:
  Delta Explain
  Gamma Explain
  Theta Explain
```

This naturally produces measure columns such as:

```text
Book Delta Explain | Book Gamma Explain | Book Theta Explain | Hedge Delta Explain | ...
```

That is correct for many users, but not for all report layouts.

## Required Business Layout

Some report layouts need the measure names to appear as row members:

```text
Category | Asset Class  | Curve/Asset | Explain Line  | Book/Saldo | Hedge
Equity   | EquityModels | EQTY ABC    | Delta Explain | 100        | -80
Equity   | EquityModels | EQTY ABC    | Gamma Explain | 25         | -10
Equity   | EquityModels | EQTY ABC    | Theta Explain | 5          | 2
```

In this layout, `Gamma Explain` for Equity is not a special hardcoded row. It is a virtual row generated from the selected measure list because the user placed measures on the row axis.

## Proposed Generic Library Feature

Add support for placing selected measures on a dimension axis.

Conceptual API options:

```text
measureAxis = "rows" | "columns" | "none"
measureNameField = "Measure Name"
measureValueField = "Amount"
```

or equivalent names:

```text
measurePlacement = "row" | "column" | "value"
measureLabelField = "Explain Line"
measureAmountField = "Amount"
```

The exact naming is flexible. The important capability is that the pivot planner can transform selected wide measures into a virtual long relation before aggregation.

## Example Configuration

For the Explain Greeks report:

```text
Rows:
  Category
  Asset Class
  Curve/Asset
  Measure Name

Columns:
  Portfolio

Values:
  Amount

Selected source measures:
  Delta Explain
  Gamma Explain
  Theta Explain
  Increment

measureAxis:
  rows
```

The library should internally treat this as if the source data were transformed into:

```text
Category | Asset Class  | Curve/Asset | Portfolio | Measure Name  | Amount
Equity   | EquityModels | EQTY ABC    | Book      | Delta Explain | 100
Equity   | EquityModels | EQTY ABC    | Book      | Gamma Explain | 25
Equity   | EquityModels | EQTY ABC    | Book      | Theta Explain | 5
Equity   | EquityModels | EQTY ABC    | Hedge     | Delta Explain | -80
Equity   | EquityModels | EQTY ABC    | Hedge     | Gamma Explain | -10
Equity   | EquityModels | EQTY ABC    | Hedge     | Theta Explain | 2
```

Then the normal pivot engine can aggregate:

```text
Rows:
  Category
  Asset Class
  Curve/Asset
  Measure Name

Columns:
  Portfolio

Value:
  sum(Amount)
```

## SQL Implementation Model

The source table should not need to be physically mutated. The library can generate a virtual subquery using DuckDB `UNPIVOT` or `UNION ALL`.

Example with `UNION ALL`:

```sql
SELECT
  Category,
  "Asset Class",
  "Curve/Asset",
  Portfolio,
  'Delta Explain' AS "Measure Name",
  "Delta Explain" AS Amount
FROM source

UNION ALL

SELECT
  Category,
  "Asset Class",
  "Curve/Asset",
  Portfolio,
  'Gamma Explain' AS "Measure Name",
  "Gamma Explain" AS Amount
FROM source

UNION ALL

SELECT
  Category,
  "Asset Class",
  "Curve/Asset",
  Portfolio,
  'Theta Explain' AS "Measure Name",
  "Theta Explain" AS Amount
FROM source;
```

Equivalent DuckDB `UNPIVOT` is also acceptable if it integrates cleanly with the existing planner.

## Why This Belongs In The Pivot Library

This is a generic pivot-table capability, not Greeks-specific business logic.

Different users may want the same physical measures in different orientations:

- User A wants `Delta Explain`, `Gamma Explain`, and `Theta Explain` as separate value columns.
- User B wants those same measures as rows under an `Explain Line` dimension.
- User C may want the measure axis on columns but with custom ordering and aliases.

Hardcoding a custom DuckDB view for one user would solve one report but would not scale architecturally. The reusable library capability should be: selected measures can become members of a row or column axis.

## What Should Stay In The Application Layer

The application should define business semantics and saved/report presets, not generic pivot mechanics.

Omega/Greeks can provide fields such as:

```text
Category
Asset Class
Curve/Asset
Portfolio
Period
Sort fields
```

The app can also provide report-specific labels and ordering:

```text
Portfolio = Book  -> Saldo
Portfolio = Hedge -> Hedge
Delta Explain before Gamma Explain before Theta Explain
Saldo before Hedge
```

These are business/report presets and should remain configurable per user or per saved view.

## What Should Stay In The Library Layer

The library should provide generic mechanics:

```text
- Measures as rows
- Measures as columns
- Measure label field
- Measure value field
- Ordering of virtual measure members
- Optional aliases for virtual measure members
- Optional suppression of zero/empty virtual measure rows
```

## Zero Or Empty Measure Rows

Some selected measures may be irrelevant for a given asset class or aggregate to zero.

The library should ideally support a configurable policy:

```text
showZeroMeasureRows = true | false
```

or:

```text
suppressEmptyMeasureMembers = true | false
```

For a stable financial report, users may want all selected measure rows visible even if zero. For exploratory analysis, users may prefer empty rows suppressed.

## Equity Gamma Example

No Equity-specific rule should be required.

Given wide data:

```text
Category | Asset Class  | Curve/Asset | Portfolio | Gamma Explain
Equity   | EquityModels | EQTY ABC    | Book      | 25
Equity   | EquityModels | EQTY ABC    | Hedge     | -10
```

If the user selects:

```text
Selected measures:
  Delta Explain
  Gamma Explain
  Theta Explain

measureAxis:
  rows
```

then the library creates virtual rows:

```text
Measure Name   | Portfolio | Amount
Gamma Explain  | Book      | 25
Gamma Explain  | Hedge     | -10
```

The final report naturally shows:

```text
Equity -> EQTY ABC -> Gamma Explain -> Book/Saldo = 25, Hedge = -10
```

This is the desired behavior.

## Recommended Final Architecture

Use four layers:

1. Raw/prepared data layer
   - Existing Explain Greeks facts.

2. Semantic application layer
   - Adds reusable business dimensions such as `Category`, `Asset Class`, `Curve/Asset`, `Period`, and sort fields.

3. Generic pivot-library layer
   - Supports measure-as-dimension behavior by virtually unpivoting selected measures.

4. User/report preset layer
   - Stores each user's preferred layout: rows, columns, selected measures, measure axis, aliases, ordering, filters, totals, and zero-row policy.

## Recommendation

Implement measure-as-dimension in the pivot library as a generic capability.

Do not create one hardcoded view per user layout unless it is only a cached/materialized optimization behind the same generic API.

The application should describe what the user wants. The pivot library should decide how to execute it efficiently in DuckDB.
