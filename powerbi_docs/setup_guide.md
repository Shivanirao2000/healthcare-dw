# Power BI Setup Guide — Healthcare Data Warehouse

Connect Power BI Desktop to the five Snowflake mart tables, model the star
schema, write all DAX measures, configure Row-Level Security, and build the
three-page dashboard.

**Power BI Desktop version required:** March 2024 or later (Snowflake connector
with OAuth support).  
**Snowflake objects required:** Run `snowflake_setup.sql` and a full dbt run
(`make dbt-run`) before proceeding.

---

## Table of Contents

1. [Connect to Snowflake](#1-connect-to-snowflake)
2. [Load the Five Mart Tables](#2-load-the-five-mart-tables)
3. [Build the Star Schema in Model View](#3-build-the-star-schema-in-model-view)
4. [DAX Measures](#4-dax-measures)
5. [Row-Level Security](#5-row-level-security)
6. [Three-Page Dashboard Layout](#6-three-page-dashboard-layout)

---

## 1. Connect to Snowflake

### 1.1 Open the Snowflake connector

1. Launch **Power BI Desktop**.
2. Click **Home → Get Data → More…**
3. In the search box type `Snowflake`, select **Snowflake**, click **Connect**.

### 1.2 Enter connection details

| Field | Value | Notes |
|---|---|---|
| **Server** | `<account_identifier>.snowflakecomputing.com` | Find in Snowflake UI: bottom-left account menu → Copy account URL. Include the full host, e.g. `xy12345.us-east-1.aws.snowflakecomputing.com` |
| **Warehouse** | `HEALTHCARE_WH` | The X-SMALL warehouse from `snowflake_setup.sql`. Power BI import queries run here. |
| **Database** | `HEALTHCARE_DW` | The production database. For dev use `DEV_HEALTHCARE_DW` (zero-copy clone). |
| **Schema** | `MARTS` | Pre-filter the navigator to only show mart tables. Leave blank to browse all schemas. |
| **Role** | `REPORTER` | The read-only role granted `SELECT` on all MARTS tables. Never use `ACCOUNTADMIN`. |
| **Data Connectivity mode** | **Import** (Import mode) | See trade-off note below. |

> **Import vs DirectQuery trade-off**  
> **Import** (recommended for this dataset): Power BI loads all five mart tables into
> an in-memory columnar store (VertiPaq). Queries are extremely fast because no
> network round-trip happens on each visual interaction. The trade-off is that
> data is a snapshot from the last refresh time — schedule **daily at 07:00 UTC**
> in the Power BI Service to stay in sync with the nightly Airflow run.  
>
> **DirectQuery**: every visual sends a SQL query to Snowflake in real time.
> Always current, but each dashboard interaction bills Snowflake compute credits
> and introduces 2–10 second latency per visual. Appropriate only if CMS data
> changes more frequently than once per day (it doesn't).

### 1.3 Authenticate

Power BI will prompt for credentials. Use **Username/Password** with the
Snowflake user that holds the `REPORTER` role, or configure
**Microsoft Entra ID (SSO)** if your Snowflake account has AAD integration
enabled. For service refresh in Power BI Service, store credentials in a
**Gateway data source** connection.

Click **Connect**.

---

## 2. Load the Five Mart Tables

### 2.1 Navigate the table selector

After connecting, the **Navigator** pane shows the MARTS schema. Check the box
next to each of the following tables:

| Table | Rows (approx.) | Purpose |
|---|---|---|
| `FCT_CLAIMS` | ~9.5 M | Fact table — one row per provider × procedure × year |
| `DIM_PROVIDERS` | ~1.2 M | Provider NPI, name, specialty, location |
| `DIM_PROCEDURES` | ~5 000 | HCPCS code, description, category |
| `DIM_GEOGRAPHY` | ~55 | State, Census region, urban/rural |
| `DIM_DATE` | 2 192 | Date spine 2019-01-01 → 2024-12-31 |

### 2.2 Preview before loading

Click any table name (not the checkbox) to preview the first 1 000 rows.
Verify:
- `FCT_CLAIMS` has columns `total_medicare_payment`, `total_services`,
  `provider_npi`, `hcpcs_code`, `state_abbr`, `data_year`
- `DIM_DATE` has `date_day`, `year`, `month_of_year`, `month_name`

### 2.3 Transform before load — correct data types

Click **Transform Data** instead of Load to open Power Query Editor.

Apply these type corrections:

**FCT_CLAIMS:**
| Column | Set type to |
|---|---|
| `claim_key` | Text |
| `provider_npi` | Text |
| `hcpcs_code` | Text |
| `state_abbr` | Text |
| `data_year` | Whole Number |
| `total_services` | Decimal Number |
| `beneficiary_count` | Whole Number |
| `total_medicare_payment` | Fixed Decimal Number (currency) |
| `total_submitted_charges` | Fixed Decimal Number |
| `total_medicare_allowed` | Fixed Decimal Number |
| `total_medicare_standardized` | Fixed Decimal Number |
| `avg_medicare_payment` | Decimal Number |
| `avg_submitted_charge` | Decimal Number | Referenced as `fct_claims[avg_submitted_charge]` in scatter tooltips |
| `total_submitted_charges` | Fixed Decimal Number | Referenced as `fct_claims[total_submitted_charges]` |
| `total_medicare_standardized` | Fixed Decimal Number | Referenced as `fct_claims[total_medicare_standardized]` |
| `claim_key` | Text | Referenced as `fct_claims[claim_key]` — surrogate PK, used to count distinct claims |

**DIM_DATE:**
| Column | Set type to |
|---|---|
| `date_day` | Date |
| `year` | Whole Number |
| `month_of_year` | Whole Number |
| `quarter_of_year` | Whole Number |
| `fiscal_year` | Whole Number |
| `is_weekend` | True/False |

Click **Close & Apply** to load all five tables into the VertiPaq in-memory engine.

---

## 3. Build the Star Schema in Model View

### 3.1 Open Model view

Click the **Model** icon on the left sidebar (looks like three connected shapes).
Arrange tables so `FCT_CLAIMS` is in the centre and the four dimensions surround it
— this is the classic "bus star" layout Power BI expects for optimal filter propagation.

### 3.2 Create the date bridge column (required)

> **Why this is necessary:** `fct_claims[data_year]` is an integer (e.g. 2022)
> but `dim_date[date_day]` is a Date. Power BI cannot create a relationship
> between mismatched types. The solution is a calculated column that converts
> the integer year into the January 1 date of that year, which exists in
> `dim_date`.

In Model view, right-click **FCT_CLAIMS → New column**. Enter:

```dax
date_key =
DATE ( fct_claims[data_year], 1, 1 )
```

This adds a `date_key` column where every row in `FCT_CLAIMS` gets a Date value
representing January 1 of its data year (e.g. `2022-01-01`). Since `DIM_DATE`
contains every day from 2019-01-01 to 2024-12-31, each `date_key` value
exists exactly once in `dim_date[date_day]` — making it a valid many-to-one join.

### 3.3 Create the four relationships

In Model view, drag from the **many side** (FCT_CLAIMS) to the **one side** (dim table).
Configure each relationship as follows:

#### Relationship 1: Provider dimension

| Setting | Value |
|---|---|
| From | `fct_claims[provider_npi]` |
| To | `dim_providers[provider_npi]` |
| Cardinality | Many to One (★ → 1) |
| Cross-filter direction | **Single** (DIM_PROVIDERS filters FCT_CLAIMS) |
| Active | ✅ Yes |

#### Relationship 2: Procedure dimension

| Setting | Value |
|---|---|
| From | `fct_claims[hcpcs_code]` |
| To | `dim_procedures[hcpcs_code]` |
| Cardinality | Many to One |
| Cross-filter direction | Single |
| Active | ✅ Yes |

#### Relationship 3: Geography dimension

| Setting | Value |
|---|---|
| From | `fct_claims[state_abbr]` |
| To | `dim_geography[state_abbr]` |
| Cardinality | Many to One |
| Cross-filter direction | Single |
| Active | ✅ Yes |

#### Relationship 4: Date dimension

| Setting | Value |
|---|---|
| From | `fct_claims[date_key]` (the calculated column from 3.2) |
| To | `DIM_DATE[date_day]` |
| Cardinality | Many to One |
| Cross-filter direction | Single |
| Active | ✅ Yes |

> **Cross-filter direction note:** Keep all four relationships as **Single**
> (dimension → fact direction). Enabling bi-directional cross-filtering on a
> star schema causes ambiguous filter paths — if both DIM_GEOGRAPHY and
> DIM_PROVIDERS can filter each other through FCT_CLAIMS, Power BI may choose
> the wrong path and return wrong numbers. Single direction is unambiguous.

### 3.4 Verify the schema

Your completed model should look like this:

```
              DIM_DATE
              [date_day]
                  │ 1
                  │
DIM_PROVIDERS ─── FCT_CLAIMS ─── DIM_PROCEDURES
[provider_npi] ★  [claim_key]  ★ [hcpcs_code]
                  [provider_npi]
                  [hcpcs_code]
                  [state_abbr]
                  [date_key]
                  ★
                  │
              DIM_GEOGRAPHY
              [state_abbr]
```

Confirm in the **Properties** pane of each relationship:
- Cardinality shows `Many to one (*:1)`
- Cross-filter shows `Single`
- The relationship line is **solid** (not dashed, which would indicate inactive)

### 3.5 Set data categorisation on geographic columns

Select `DIM_GEOGRAPHY` → click `state_name` column → in the Column tools ribbon:
- **Data category → State or Province**

Select `state_abbr` → **Data category → State or Province**

These settings enable Power BI's filled map visual to automatically geocode
US states without a custom latitude/longitude feed.

---

## 4. DAX Measures

All measures below belong in the `FCT_CLAIMS` table unless noted. To create a
measure: right-click **FCT_CLAIMS** in the Fields pane → **New measure**.

Name every measure in `[Square brackets]` style (Power BI convention).

### 4.1 Total Medicare Payment

The base measure that all other measures reference. Create this first.

```dax
Total Medicare Payment =
SUM ( fct_claims[total_medicare_payment] )
```

Format: Currency, 0 decimal places.  
**What it counts:** Sum of estimated Medicare payments across all
provider-procedure-year combinations in the current filter context.

---

### 4.2 Total Services

```dax
Total Services =
SUM ( fct_claims[total_services] )
```

Format: Whole number, comma separator.  
**Grain note:** `total_services` is the sum of billed service units per
provider-HCPCS-year row. This is not a count of individual claims — it is
an aggregate of billed units (e.g. 1 unit of 99213 = 1 office visit).

---

### 4.3 Beneficiary Count

```dax
Beneficiary Count =
SUM ( fct_claims[beneficiary_count] )
```

Format: Whole number, comma separator.  
**Important caveat:** `beneficiary_count` in each row is `MAX(TOT_BENES)`
across the facility/office split (see `fct_claims.sql`). Summing across multiple
provider-procedure rows will double-count beneficiaries who received the same
procedure from multiple providers. Use this measure for totals within a single
provider-procedure slice; interpret cross-provider totals as upper bounds.

---

### 4.4 YTD Reimbursement

Cumulative Medicare payment from January 1 of the current year to the date
selected in a slicer or filter context.

```dax
YTD Reimbursement =
TOTALYTD (
    SUM ( fct_claims[total_medicare_payment] ),
    dim_date[date_day]
    -- Optional: change fiscal year-end to September 30 for CMS/HHS fiscal year:
    -- , "09-30"
)
```

**How TOTALYTD works internally:** It is equivalent to:
```dax
-- Equivalent long-form (shown for clarity, use TOTALYTD above in practice)
YTD Reimbursement (expanded) =
CALCULATE (
    SUM ( fct_claims[total_medicare_payment] ),
    DATESYTD ( dim_date[date_day] )
)
```
`DATESYTD` expands the current filter on `dim_date[date_day]` to include all
dates from January 1 of the selected year through the maximum date in the
current context. Since `FCT_CLAIMS` joins to `DIM_DATE` through the `date_key`
calculated column, this filter propagates correctly into the fact table.

**Data grain note:** CMS data is annual. When filtered to year 2022 in a slicer,
`YTD Reimbursement` returns the full 2022 total (because `date_key` is always
January 1). When no year filter is applied, it accumulates all years through
December 31 of the most recent year in context.

**Fiscal year variant:** To align with CMS's October 1 fiscal year-end, pass
`"09-30"` as the third argument. Then a date of `2024-01-15` accumulates from
`2023-10-01` through `2024-01-15`.

Format: Currency, 0 decimal places.

---

### 4.5 MoM Growth %

Month-over-month (or year-over-year with annual CMS data) percentage change in
Medicare payments.

```dax
MoM Growth % =
VAR CurrentPeriodPayment =
    [Total Medicare Payment]
VAR PriorPeriodPayment =
    CALCULATE (
        [Total Medicare Payment],
        DATEADD ( dim_date[date_day], -1, MONTH )
    )
VAR Result =
    DIVIDE (
        CurrentPeriodPayment - PriorPeriodPayment,
        PriorPeriodPayment,
        BLANK ()   -- return BLANK (not 0) when prior period has no data
                   -- so the measure disappears from visuals rather than
                   -- showing a misleading 0% change
    )
RETURN
    Result
```

**How DATEADD works here:** `DATEADD ( dim_date[date_day], -1, MONTH )` shifts
the current date filter context back by one month. The measure evaluates
`[Total Medicare Payment]` within that shifted context — the prior period total.

**Data grain reality:** CMS data exists only at annual grain (one `date_key`
per year = January 1). With a year slicer showing 2022, `DATEADD(-1, MONTH)`
shifts to December 1, 2021 — a date that does not have CMS claims data attached
to it. This means the measure shows a **year-over-year** comparison when used
with the year slicer (select 2022 → compares to 2021 full-year total), which
is the analytically correct interpretation for annual CMS data.

For true month-over-month trending: this measure works correctly if you overlay
monthly claims data from a source with sub-annual grain (e.g. the Airflow
`STG_CLAIMS_INCREMENTAL` stream table).

Format: Percentage, 1 decimal place. Apply conditional formatting (green for
positive, red for negative) in the Format pane → Cell elements → Background color.

---

### 4.6 Avg Cost Per Claim

Volume-weighted average Medicare payment per billed service unit.

```dax
Avg Cost Per Claim =
DIVIDE (
    SUM ( fct_claims[total_medicare_payment] ),
    SUM ( fct_claims[total_services] ),
    BLANK ()   -- BLANK instead of 0: suppresses the measure in visuals when
               -- there are no services (e.g. a filtered provider with no claims)
)
```

**Why `BLANK()` not `0` in DIVIDE:** Passing `BLANK()` as the alternative result
causes visuals (bar charts, tables) to skip that data point entirely rather than
plotting a zero bar. For scatter plots showing provider cost efficiency, a zero
would be indistinguishable from a legitimate zero-payment provider. `BLANK()` is
always the right default for ratio measures.

**Interpretation:** This is the average Medicare payment per service unit billed
by the provider-procedure combination in the current filter context. For example,
if a cardiologist billed 500 echocardiogram units totalling $250,000, this
measure returns $500 per service.

Format: Currency, 2 decimal places.

---

### 4.7 Provider Share %

Each provider's share of total Medicare payment, computed within the current
filter context (e.g. within a specialty or state slicer).

```dax
Provider Share % =
VAR ProviderContextPayment =
    [Total Medicare Payment]
VAR AllProvidersPayment =
    CALCULATE (
        [Total Medicare Payment],
        ALL ( dim_providers )   -- removes ALL filters on DIM_PROVIDERS so the
                                -- denominator is the total across every provider
                                -- still filtered by procedure, geography, and year
    )
RETURN
    DIVIDE (
        ProviderContextPayment,
        AllProvidersPayment,
        BLANK ()
    )
```

**What `ALL(dim_providers)` does:** It clears the row context imposed by
DIM_PROVIDERS (for example, when a table visual iterates row-by-row over providers)
while preserving all other active filters — date year, procedure category, state.
The result: each provider's share is computed relative to the total for the same
procedure/geography/year context, not the absolute total across everything.

**Example interpretation:** In a table filtered to "Cardiology" providers in
"Northeast" for 2022, Provider Share % sums to 100% within that slice.

Format: Percentage, 2 decimal places.

---

### Measure reference summary

| Measure | Format | Table |
|---|---|---|
| `[Total Medicare Payment]` | Currency $0 | FCT_CLAIMS |
| `[Total Services]` | #,0 | FCT_CLAIMS |
| `[Beneficiary Count]` | #,0 | FCT_CLAIMS |
| `[YTD Reimbursement]` | Currency $0 | FCT_CLAIMS |
| `[MoM Growth %]` | % 0.0 | FCT_CLAIMS |
| `[Avg Cost Per Claim]` | Currency $0.00 | FCT_CLAIMS |
| `[Provider Share %]` | % 0.00 | FCT_CLAIMS |

---

## 5. Row-Level Security

Power BI RLS restricts which rows a user sees based on their identity
(`USERPRINCIPALNAME()` returns the signed-in user's email address in Power BI Service).

### 5.1 Create the security mapping tables

RLS filters work by comparing `USERPRINCIPALNAME()` against a mapping table.
Create two small mapping tables — either as Power Query tables embedded in the
`.pbix` file, or as Snowflake tables loaded via the same connection.

#### UserRegionMapping table

| Column | Type | Example values |
|---|---|---|
| `user_principal_name` | Text | `jane.smith@example.com` |
| `allowed_region` | Text | `Northeast` or `ALL` |

One row per user-region assignment. A user with `allowed_region = 'ALL'` bypasses
the region filter (use for national analysts and admin roles).

#### UserFacilityMapping table

| Column | Type | Example values |
|---|---|---|
| `user_principal_name` | Text | `john.doe@example.com` |
| `allowed_entity_type` | Text | `Individual` or `Organization` or `ALL` |

Load both tables via **Get Data → Enter Data** (for small static lists) or via
the same Snowflake connection pointing to `MARTS.USER_REGION_MAPPING` and
`MARTS.USER_FACILITY_MAPPING` if you manage access lists in the warehouse.

**Do not** create relationships from these security tables to the dimension
tables — RLS filters are applied before relationships, so a relationship is
unnecessary and could expose data.

### 5.2 Define the GeoRegion role

1. Click **Modeling tab → Manage roles → + New**.
2. Name the role **GeoRegion**.
3. Select table **DIM_GEOGRAPHY**.
4. Enter the DAX filter expression:

```dax
[census_region] = LOOKUPVALUE (
    UserRegionMapping[allowed_region],
    UserRegionMapping[user_principal_name], USERPRINCIPALNAME (),
    "ALL"   -- default: if user not found in mapping, grant no access
            -- change to a specific region string to make "no match = default region"
)
||
LOOKUPVALUE (
    UserRegionMapping[allowed_region],
    UserRegionMapping[user_principal_name], USERPRINCIPALNAME (),
    "ALL"
) = "ALL"
```

**What this expression does, step by step:**

- `LOOKUPVALUE(...)` searches `UserRegionMapping` for the current user's UPN
  and returns their assigned `allowed_region`.
- The `||` (OR) clause allows users with `allowed_region = "ALL"` to see every
  region — their row passes the filter regardless of `[census_region]`.
- If the user is not in the mapping table, `LOOKUPVALUE` returns the default
  value `"ALL"`, which means they see no regions (because no `census_region`
  equals `"ALL"`). Adjust the default to a specific region if you want unlisted
  users to fall back to a region rather than seeing nothing.

Because `FCT_CLAIMS` joins to `DIM_GEOGRAPHY` via `state_abbr`, and the
relationship cross-filter is **Single** (DIM_GEOGRAPHY → FCT_CLAIMS), the RLS
filter on `DIM_GEOGRAPHY` automatically propagates into `FCT_CLAIMS` — rows for
states outside the user's allowed region are excluded from all measures.

### 5.3 Define the FacilityType role

1. In **Manage roles**, click **+ New** again.
2. Name the role **FacilityType**.
3. Select table **DIM_PROVIDERS**.
4. Enter the DAX filter expression:

```dax
[entity_type] = LOOKUPVALUE (
    UserFacilityMapping[allowed_entity_type],
    UserFacilityMapping[user_principal_name], USERPRINCIPALNAME (),
    "ALL"
)
||
LOOKUPVALUE (
    UserFacilityMapping[allowed_entity_type],
    UserFacilityMapping[user_principal_name], USERPRINCIPALNAME (),
    "ALL"
) = "ALL"
```

`dim_providers[entity_type]` contains `Individual`, `Organization`, or `Unknown`.

- A user with `allowed_entity_type = "Individual"` only sees individual
  provider (physician/NP/PA) rows in all visuals.
- A user with `allowed_entity_type = "Organization"` only sees organizational
  providers (hospitals, group practices).
- A user with `allowed_entity_type = "ALL"` sees all rows (national analysts,
  leadership).

Because `FCT_CLAIMS` joins to `DIM_PROVIDERS` via `provider_npi`, this filter
propagates into the fact table — claims for excluded provider types disappear
from all charts and totals.

### 5.4 Assign roles to Entra ID security groups

After publishing the `.pbix` to Power BI Service:

1. Open the **dataset** in Power BI Service.
2. Click **Security**.
3. For role **GeoRegion**: add the security group `sg-healthcare-regional-analysts`.
4. For role **FacilityType**: add `sg-healthcare-facility-analysts`.
5. Users in both groups simultaneously get the intersection of both filters
   (both DIM_GEOGRAPHY and DIM_PROVIDERS are filtered in the same session).

### 5.5 Test RLS before publishing

In Power BI Desktop, click **Modeling → View as** → select a role, enter a test
email address. All visuals will recalculate under that identity's filter. Verify:
- KPI cards and bar charts show only data for the allowed region or entity type.
- The total on Page 1 changes to match the restricted subset.
- `[Provider Share %]` still sums to 100% within the visible subset — because
  `ALL(dim_providers)` removes the RLS row filter when calculating the denominator.
  If this behaviour is undesirable (you want share % relative to all providers
  nationally), replace `ALL(dim_providers)` with `ALLSELECTED(dim_providers)`.

---

## 6. Three-Page Dashboard Layout

### Page 1 — Executive Overview

**Purpose:** Single-screen summary for leadership. Answer: "How much did CMS
pay, to whom, and is it trending up or down?"

**Canvas size:** 1920 × 1080 px (16:9 widescreen). Set under **View → Page view → Actual size** while designing; switch to **Fit to page** for review.

#### Layout grid

```
┌──────────────────────────────────────────────────────────────────────────────┐
│  HEADER BAR — "Healthcare DW | CMS Medicare Analytics | FY 2022"            │
│  (Text box, dark teal background, white text, logo image)                   │
├──────────────┬──────────────┬──────────────┬────────────────────────────────┤
│  KPI CARD 1  │  KPI CARD 2  │  KPI CARD 3  │                                │
│  YTD         │  MoM         │  Total       │  YEAR SLICER                   │
│  Reimburse.  │  Growth %    │  Providers   │  (dropdown, 2019–2024)         │
│  $X.XB       │  +X.X%       │  X,XXX       │                                │
├──────────────┴──────────────┴──────────────┤                                │
│                                            │  SPECIALTY SLICER              │
│  BAR CHART                                 │  (vertical list, multi-select) │
│  "Top 10 Procedures by Medicare Payment"   │                                │
│  X-axis: dim_procedures[hcpcs_description] │                                │
│  Y-axis: [Total Medicare Payment]          │────────────────────────────────┤
│  Sort: descending by value                 │                                │
│  Top N filter: 10 by [Total Medicare Pay.] │  CENSUS REGION SLICER          │
│  Data labels: on, currency format          │  (vertical list, multi-select) │
│                                            │                                │
├────────────────────────────────────────────┤                                │
│                                            │                                │
│  LINE CHART                                │  ENTITY TYPE SLICER            │
│  "Annual Payment Trend (2019–2024)"        │  Individual / Organization     │
│  X-axis: dim_date[year]                    │                                │
│  Y-axis: [Total Medicare Payment]          │                                │
│  Secondary Y: [Beneficiary Count]          │                                │
│  Markers: on, labels on endpoint           │                                │
│                                            │                                │
└────────────────────────────────────────────┴────────────────────────────────┘
```

#### KPI Card specifications

**Card 1 — YTD Reimbursement**
- Value field: `[YTD Reimbursement]`
- Label: `YTD Medicare Reimbursement`
- Format: `$#,0,,\B` (billions) or `$#,0,\M` (millions) — set via Format pane → Data label → Display units: Auto

**Card 2 — MoM Growth %**
- Value field: `[MoM Growth %]`
- Label: `Year-over-Year Growth`
- Format: `+0.0%;-0.0%;—` (show sign, dash for zero)
- Conditional formatting: font colour green if > 0, red if < 0 (Format → Callout value → Conditional formatting → Font color → Field value)

**Card 3 — Total Providers**

Create this measure first:
```dax
Total Providers =
DISTINCTCOUNT ( fct_claims[provider_npi] )
```
- Value field: `[Total Providers]`
- Label: `Unique Billing Providers`
- Format: `#,0`

#### Bar chart — Top 10 Procedures

- Visual type: **Clustered bar chart** (horizontal bars make long HCPCS descriptions legible)
- **Y-axis (category):** `dim_procedures[hcpcs_description]`
- **X-axis (values):** `[Total Medicare Payment]`
- **Filters pane → Top N filter:**
  - Filter type: Top N
  - Show items: Top 10
  - By value: `[Total Medicare Payment]`
- **Sort:** Sort descending by `[Total Medicare Payment]`
- **Data labels:** On, value format Currency $0M
- **Tooltip fields:** add `[Total Services]`, `[Beneficiary Count]`, `dim_procedures[hcpcs_category]`

#### Line chart — Annual Payment Trend

- Visual type: **Line and clustered column chart** (dual-axis)
- **X-axis:** `dim_date[year]` — drag from DIM_DATE, not from FCT_CLAIMS
- **Column Y-axis (primary):** `[Total Medicare Payment]`
- **Line Y-axis (secondary):** `[Beneficiary Count]`
- **Legend:** None (single line, single bar series — no legend needed)
- **Markers:** On for the line series
- **Zoom slider:** Enable (View → Sync slicers is not needed on Page 1 slicers)

> **Date hierarchy note:** When you add `dim_date[year]` to the X-axis, Power BI
> may auto-expand to a full date hierarchy (Year → Quarter → Month → Day). Click
> the **X-axis** field well → select `year` (not the date hierarchy) to pin it at
> the annual level. This matches the annual grain of `FCT_CLAIMS`.

---

### Page 2 — Geographic Analysis

**Purpose:** Show which states and regions drive Medicare spend, and let
users drill through to the provider list for any state.

#### Layout grid

```
┌──────────────────────────────────────────────────────────────────────────────┐
│  HEADER + SLICERS: Year  |  Census Region  |  Urban/Rural Classification    │
├────────────────────────────────────────┬─────────────────────────────────────┤
│                                        │  DATA TABLE                        │
│  FILLED MAP                            │  State | Providers | Payment | Avg  │
│  Location: dim_geography[state_name]   │  Payment | Share %                 │
│  (set Data category = State/Province)  │  Sort: [Total Medicare Payment] ↓  │
│  Color saturation: [Total Medicare     │  Row subtotals: off                 │
│  Payment]                              │  Conditional formatting on Payment: │
│  Tooltips: [Total Providers],          │  data bar, blue gradient            │
│  [Avg Cost Per Claim],                 │                                     │
│  dim_geography[census_region],         │  ← Enable drill-through to         │
│  [Provider Share %]                    │     Page 3 on state_name column     │
│                                        │                                     │
│  Bubbles: off (filled map, not         │                                     │
│  bubble map — uses state boundaries)   │                                     │
├────────────────────────────────────────┴─────────────────────────────────────┤
│  DETAIL TABLE (appears on drill-through from map or state table row)         │
│  Columns: provider_name | specialty | city | total_services |                │
│           total_medicare_payment | avg_cost_per_claim                        │
│  Default: hidden (visible only after drill-through activation)               │
└──────────────────────────────────────────────────────────────────────────────┘
```

#### Filled map configuration

1. Add a **Filled map** visual.
2. **Location:** `dim_geography[state_name]`  
   - Power BI geocodes US state names automatically.
   - If geocoding fails, add `dim_geography[state_abbr]` to **Location** as a
     backup and set its Data category to **State or Province**.
3. **Color saturation:** `[Total Medicare Payment]` — Power BI shades each state
   polygon from light (low spend) to dark (high spend).
4. **Map style:** Road (shows state borders clearly). Light theme preferred for
   print-ready exports.
5. **Tooltip fields:**
   - `dim_geography[census_region]`
   - `dim_geography[urban_rural_classification]`
   - `[Total Providers]`
   - `[Total Medicare Payment]`
   - `[Avg Cost Per Claim]`
   - `[Provider Share %]`

#### Drill-through to provider list (Page 3)

1. Select the summary table on Page 2.
2. In the **Format** pane → **General → Cross-report drill-through** — keep off
   (same-report drill-through is sufficient).
3. Navigate to **Page 3** → Drag `dim_providers[state_abbr]` to the
   **Drill-through** filter well in the Visualisations pane.
4. Power BI will automatically add a back-navigation button on Page 3.

Now when a user right-clicks any state row in the Page 2 summary table or clicks
a state on the filled map, they can select **Drill through → Page 3 — Provider
Deep Dive** and land on Page 3 pre-filtered to that state's providers.

---

### Page 3 — Provider Deep Dive

**Purpose:** Analyst-facing page for comparing provider efficiency, finding
outliers, and investigating individual providers.

#### Layout grid

```
┌──────────────────────────────────────────────────────────────────────────────┐
│  SLICERS: Year | Specialty (multi-select on dim_providers[specialty]) |     │
│           Entity Type | Urban/Rural (dim_providers[urban_rural_classification])│
│  SEARCH SLICER: dim_providers[provider_name]  (type-ahead text search)      │
├────────────────────────────────────────┬─────────────────────────────────────┤
│                                        │  TOP / BOTTOM 10 TABLE             │
│  SCATTER PLOT                          │  Tabs: "Top 10" / "Bottom 10"      │
│  X-axis: [Total Services]              │  Columns:                          │
│  Y-axis: [Avg Cost Per Claim]          │  provider_name | specialty |       │
│  Size: [Beneficiary Count]             │  total_medicare_payment |          │
│  Legend: dim_providers[specialty]      │  total_services |                  │
│          (limit to 8 categories;       │  avg_cost_per_claim |             │
│           group rest as "Other")       │  provider_share_pct               │
│                                        │                                     │
│  Each point = one provider-procedure   │  Top 10 filter: by [Total Medicare │
│  combination in filter context.        │  Payment], descending              │
│                                        │  Bottom 10: ascending              │
│  Reference lines:                      │                                     │
│  • X-axis median line: MEDIAN of       │  Conditional bar on payment col.   │
│    [Total Services] (DAX constant line)│                                     │
│  • Y-axis median line: MEDIAN of       │                                     │
│    [Avg Cost Per Claim]                │                                     │
│                                        │                                     │
│  Tooltips: provider_name, specialty,  │                                     │
│  city, state_abbr, [Provider Share %] │                                     │
│                                        │                                     │
├────────────────────────────────────────┴─────────────────────────────────────┤
│  PROVIDER DETAIL CARD (appears when a scatter point or table row is clicked) │
│  Shows: provider_name, credentials, specialty, entity_type,                 │
│  urban_rural_classification, medicare_participating_ind, city, zip5         │
│  Implement with: Q&A visual or a multi-row card filtered by provider_npi    │
└──────────────────────────────────────────────────────────────────────────────┘
```

#### Provider search slicer

1. Add a **Slicer** visual.
2. Field: `dim_providers[provider_name]`
3. In the **Format** pane → **Slicer settings → Style → Dropdown** — change to
   **Tile** or leave as **Dropdown** with search enabled.
4. Enable **Single select: off** (allow multi-provider comparison).
5. The slicer supports type-ahead search natively when Style = Dropdown with the
   search icon enabled (Format → Slicer header → Search box: On).

This allows analysts to type "Smith" and see all providers whose `provider_name`
contains that string — equivalent to a WHERE clause filter on the entire page.

#### Scatter plot — Services vs. Cost

The scatter plot reveals two types of outliers:
- **High services, high cost** (top-right quadrant): high-volume providers
  with above-average reimbursement — potential for cost-reduction intervention.
- **Low services, high cost** (top-left quadrant): low-volume providers
  billing at high average cost — potentially data quality issues or specialised
  procedures.

Add reference lines to mark the median of each axis:

```dax
Median Services =
MEDIAN ( fct_claims[total_services] )

Median Avg Cost Per Claim =
MEDIAN ( fct_claims[avg_medicare_payment] )
```

In the **Analytics pane** of the scatter visual:
- **X-axis constant line** → Value: `[Median Services]`
- **Y-axis constant line** → Value: `[Median Avg Cost Per Claim]`

This creates four quadrants that analysts can use to stratify the provider
population without writing additional DAX.

#### Top / Bottom 10 table

The same table visual serves both Top 10 and Bottom 10 via **Bookmarks**:

1. Create the table with a Top N filter: Top 10 by `[Total Medicare Payment]`.
2. Create a **Bookmark** named "Top 10" capturing current filter state.
3. Change the filter to Bottom 10 (ascending). Create another Bookmark "Bottom 10".
4. Add two **Buttons** (Insert → Buttons → Blank) labelled "Top 10" and
   "Bottom 10", assign the corresponding bookmarks.

Users toggle between views without navigating away from the page.

---

## Appendix A — Scheduled Refresh in Power BI Service

After publishing the `.pbix`:

1. Open the **dataset** settings in Power BI Service.
2. **Gateway connection:** configure an On-premises Data Gateway if Snowflake is
   not publicly accessible from Microsoft's datacenters. For Snowflake cloud
   accounts, direct connection (no gateway) usually works.
3. **Data source credentials:** enter the `REPORTER` role Snowflake username and
   password.
4. **Scheduled refresh:** set to **Daily**, time **07:00 UTC** — 1 hour after
   the Airflow pipeline completes at 06:00 UTC and the GE checks pass.
5. **Refresh failure notification:** enter the data engineering team email.

---

## Appendix B — Performance Optimisation Tips

| Issue | Solution |
|---|---|
| FCT_CLAIMS (9.5 M rows) slow to load | In Power Query, fold a `WHERE DATA_YEAR = 2022` filter into the Snowflake query so only the target year loads |
| Scatter plot slow with 9.5 M points | Add a **top N filter** to the scatter: Top 10,000 by `[Total Medicare Payment]` — scatter plots cannot meaningfully display 9.5 M points |
| `[Provider Share %]` slow in large tables | Pre-compute the national total as a variable using `ALLSELECTED` instead of `ALL`; consider a calculated column for static share at import time |
| Filled map geocoding fails on territories | Exclude PR, GU, VI, AS, MP from the map by adding a visual-level filter on `dim_geography[census_region] <> "Territory / Other"` |
| Bi-directional relationships warned | Keep all four relationships Single-direction; create a separate `DIM_DATE_COPY` table if you need DIM_DATE to filter through two different fact paths |

---

## Appendix C — Measure Dependency Tree

```
[Total Medicare Payment]
│
├── [YTD Reimbursement]          (TOTALYTD wraps the base measure)
├── [MoM Growth %]               (DATEADD shifts date context for prior period)
├── [Avg Cost Per Claim]         (DIVIDE by [Total Services])
├── [Provider Share %]           (DIVIDE by ALL(dim_providers) variant)
│
[Total Services]
│
└── [Avg Cost Per Claim]         (denominator)

[Beneficiary Count]              (independent SUM)

[Total Providers]                (DISTINCTCOUNT — independent)
[Median Services]                (MEDIAN — Page 3 reference line only)
[Median Avg Cost Per Claim]      (MEDIAN — Page 3 reference line only)
```

Build measures in this order: base SUM measures first, then derived measures
that reference them. Circular dependencies are not allowed in DAX.
