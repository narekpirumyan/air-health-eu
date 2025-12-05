# Power BI Dashboard Guide: EU Air & Health Analysis

This guide walks you through creating a comprehensive Power BI dashboard that visualizes greenhouse gas emissions, respiratory health outcomes, and correlations between them across EU NUTS2 regions.

## Table of Contents

1. [Prerequisites](#prerequisites)
2. [Connecting to the Database](#connecting-to-the-database)
3. [Setting Up the Data Model](#setting-up-the-data-model)
4. [Dashboard Structure](#dashboard-structure)
5. [Emissions Visualizations](#emissions-visualizations)
6. [Health Visualizations](#health-visualizations)
7. [Correlation Analysis](#correlation-analysis)
8. [Best Practices](#best-practices)

---

## Prerequisites

- **Power BI Desktop** installed
- **Database generated** (see `prod/README.md` for ETL instructions)
- **Database path**: `prod/data/warehouse/air_health_eu.db`

---

## Connecting to the Database

### Step 1: Connect to SQLite Database

1. Open Power BI Desktop
2. Click **Get Data** → **More...** → **Database** → **SQLite database**
3. Browse to: `prod/data/warehouse/air_health_eu.db`
4. Click **Open**

### Step 2: Select Views (Recommended Approach)

**We use views because they are pre-joined and contain all dimension attributes, making them ideal for Power BI without complex relationships.**

When the Navigator opens, select these views:

- ✅ **`vw_emissions`** - Emissions data with geography, time, sector, and gas dimensions
- ✅ **`vw_health_metrics`** - Causes of death data with geography, time, and ICD-10 dimensions
- ✅ **`vw_hospital_discharges`** - Hospital discharge data with geography, time, and discharge type dimensions

**Optional (for additional filtering):**
- `dim_geography` - If you need more geographic filtering options
- `dim_time` - If you need more time-based filtering options

Click **Load** to import the data.

**Why views?**
- ✅ Pre-joined data (no relationship setup needed)
- ✅ Human-readable column names
- ✅ Faster queries (pre-aggregated where possible)
- ✅ No risk of cyclic reference errors
- ✅ Simpler data model

---

## Setting Up the Data Model

### No Relationships Needed!

Since we're using views, **no relationships need to be set up**. Views are already pre-joined and contain all the dimension attributes you need.

**What's in each view:**

- **`vw_emissions`**: Contains emissions data with columns like `country_name`, `nuts_id`, `nuts_label`, `year`, `sector_name`, `sector_group`, `gas_name`, `emissions_kt_co2e`
- **`vw_health_metrics`**: Contains health data with columns like `country_name`, `nuts_id`, `nuts_label`, `year`, `icd10_code`, `icd10_name`, `icd10_category`, `is_respiratory`, `age_standardised_rate_per_100k`
- **`vw_hospital_discharges`**: Contains hospital data with columns like `country_name`, `nuts_id`, `nuts_label`, `year`, `discharge_code`, `discharge_name`, `discharge_category`, `is_respiratory`, `discharge_rate_per_100k`

You can start creating visualizations immediately!

---

## Dashboard Structure

Create a dashboard with **4 pages**:

1. **Overview** - Key metrics and trends
2. **Emissions Analysis** - GHG emissions by region, sector, gas, time
3. **Health Analysis** - Respiratory health outcomes by region, time
4. **Correlation Analysis** - Relationship between emissions and health

---

## Emissions Visualizations

### Page 2: Emissions Analysis

#### 1. Total Emissions by Country (Map or Bar Chart)

- **Visualization**: Map (filled) or Bar chart
- **Location**: `vw_emissions[country_name]` or `vw_emissions[nuts_id]` (filter to `nuts_level=0` for country level)
- **Legend**: `vw_emissions[country_name]`
- **Values**: `SUM(vw_emissions[emissions_kt_co2e])`
- **Title**: "Total GHG Emissions by Country (kt CO₂e)"
- **Filter**: Add a visual-level filter: `nuts_level = 0` to show only country-level data

#### 2. Emissions Trend Over Time (Line Chart)

- **Axis**: `vw_emissions[year]`
- **Values**: `SUM(vw_emissions[emissions_kt_co2e])`
- **Legend**: `vw_emissions[country_name]` (top 5 countries - use visual-level filter)
- **Title**: "GHG Emissions Trend (1990-2022)"
- **Format**: Add data labels, show markers
- **Filter**: Add visual-level filter: `nuts_level = 0` for country-level aggregation

#### 3. Emissions by Sector (Stacked Bar Chart)

- **Axis**: `vw_emissions[year]`
- **Legend**: `vw_emissions[sector_group]` or `vw_emissions[sector_name]`
- **Values**: `SUM(vw_emissions[emissions_kt_co2e])`
- **Title**: "Emissions by Sector Over Time"
- **Sort**: By total emissions descending

#### 4. Emissions by Gas Type (Pie Chart or Donut Chart)

- **Legend**: `vw_emissions[gas_name]` (CO₂, CH₄, N₂O, F-gas)
- **Values**: `SUM(vw_emissions[emissions_kt_co2e])`
- **Title**: "Emissions by Greenhouse Gas Type"
- **Format**: Show percentage and values

#### 5. Top 10 Regions by Emissions (Table)

- **Columns**: 
  - `vw_emissions[nuts_label]`
  - `vw_emissions[country_name]`
  - `SUM(vw_emissions[emissions_kt_co2e])` (formatted as decimal, 0 decimals)
- **Sort**: By emissions descending
- **Title**: "Top 10 Regions by Total Emissions"
- **Filter**: Add visual-level filter: Top N = 10, by `SUM(emissions_kt_co2e)`

#### 6. Emissions Slicer Panel

Create slicers for:
- **Year Range**: `vw_emissions[year]` (between 1990-2022)
- **Country**: `vw_emissions[country_name]`
- **Sector**: `vw_emissions[sector_group]`
- **Gas Type**: `vw_emissions[gas_name]`
- **NUTS Level**: `vw_emissions[nuts_level]` (0=Country, 1=NUTS1, 2=NUTS2)

---

## Health Visualizations

### Page 3: Health Analysis

#### 1. Respiratory Death Rates by Country (Map or Bar Chart)

- **Visualization**: Map (filled) or Bar chart
- **Location**: `vw_health_metrics[country_name]` (filter `nuts_level = 0` for country level)
- **Values**: `AVERAGE(vw_health_metrics[age_standardised_rate_per_100k])`
- **Title**: "Average Respiratory Death Rate by Country (per 100k)"
- **Format**: Show data labels
- **Filter**: Add visual-level filter: `is_respiratory = 1`

#### 2. Respiratory Death Rate Trend (Line Chart)

- **Axis**: `vw_health_metrics[year]` (2000-2021)
- **Values**: `AVERAGE(vw_health_metrics[age_standardised_rate_per_100k])`
- **Legend**: `vw_health_metrics[country_name]` (top 5 countries - use visual-level filter)
- **Filters**: 
  - `vw_health_metrics[is_respiratory] = 1`
  - `vw_health_metrics[nuts_level] = 0` (for country-level aggregation)
- **Title**: "Respiratory Death Rate Trend (2000-2021)"

#### 3. Death Rates by ICD-10 Category (Stacked Bar Chart)

- **Axis**: `vw_health_metrics[year]`
- **Legend**: `vw_health_metrics[icd10_category]` or `vw_health_metrics[icd10_name]`
- **Values**: `AVERAGE(vw_health_metrics[age_standardised_rate_per_100k])`
- **Filters**: 
  - `vw_health_metrics[is_respiratory] = 1`
  - `vw_health_metrics[nuts_level] = 0` (for country-level aggregation)
- **Title**: "Respiratory Death Rates by Cause Category"

#### 4. Hospital Discharge Rates by Country (Bar Chart)

- **Axis**: `vw_hospital_discharges[country_name]`
- **Values**: `AVERAGE(vw_hospital_discharges[discharge_rate_per_100k])`
- **Filters**: 
  - `vw_hospital_discharges[is_respiratory] = 1`
  - `vw_hospital_discharges[nuts_level] = 0` (for country-level aggregation)
- **Title**: "Average Respiratory Hospital Discharge Rate (per 100k)"
- **Sort**: By value descending

#### 5. Top 10 Regions by Respiratory Death Rate (Table)

- **Columns**:
  - `vw_health_metrics[nuts_label]`
  - `vw_health_metrics[country_name]`
  - `AVERAGE(vw_health_metrics[age_standardised_rate_per_100k])` (formatted as decimal, 1 decimal)
- **Filters**: 
  - `vw_health_metrics[is_respiratory] = 1`
- **Sort**: By death rate descending
- **Title**: "Top 10 Regions by Respiratory Death Rate"
- **Filter**: Add visual-level filter: Top N = 10, by `AVERAGE(age_standardised_rate_per_100k)`

#### 6. Health Slicer Panel

Create slicers for:
- **Year Range**: `vw_health_metrics[year]` (between 2000-2021)
- **Country**: `vw_health_metrics[country_name]`
- **ICD-10 Category**: `vw_health_metrics[icd10_category]`
- **Respiratory Only**: `vw_health_metrics[is_respiratory]` (toggle: 1 or 0)
- **NUTS Level**: `vw_health_metrics[nuts_level]`

---

## Correlation Analysis

### Page 4: Correlation Analysis

This page shows the relationship between emissions and health outcomes.

#### 1. Scatter Plot: Emissions vs. Respiratory Death Rate

**Step 1: Create a Calculated Table**

Since we need to join emissions and health data by region and year, create a calculated table in Power BI:

1. Go to **Model view**
2. Click **New Table** in the ribbon
3. Enter this DAX formula:

```dax
Correlation_Data = 
VAR EmissionsByRegion = 
    SUMMARIZE(
        vw_emissions,
        vw_emissions[nuts_id],
        vw_emissions[country_name],
        vw_emissions[nuts_label],
        vw_emissions[year],
        "Total_Emissions", SUM(vw_emissions[emissions_kt_co2e])
    )
VAR HealthByRegion = 
    SUMMARIZE(
        FILTER(vw_health_metrics, vw_health_metrics[is_respiratory] = 1),
        vw_health_metrics[nuts_id],
        vw_health_metrics[country_name],
        vw_health_metrics[nuts_label],
        vw_health_metrics[year],
        "Avg_Death_Rate", AVERAGE(vw_health_metrics[age_standardised_rate_per_100k])
    )
RETURN
    NATURALINNERJOIN(EmissionsByRegion, HealthByRegion)
```

**Step 2: Create the Scatter Plot**

- **X-axis**: `Correlation_Data[Total_Emissions]`
- **Y-axis**: `Correlation_Data[Avg_Death_Rate]`
- **Legend**: `Correlation_Data[country_name]`
- **Size**: Count of records (or create a measure for population if needed)
- **Title**: "Emissions vs. Respiratory Death Rate by Region"
- **Filters**: 
  - Year range: 2000-2021 (to match health data availability)
  - Optional: Filter by `nuts_level` for specific regional levels

#### 2. Correlation Matrix (Custom Visual or Table)

Use the `Correlation_Data` table created above, or create a summary table:

- **Rows**: `Correlation_Data[country_name]` or `Correlation_Data[nuts_label]`
- **Columns**: 
  - `SUM(Correlation_Data[Total_Emissions])` - Total Emissions (kt CO₂e)
  - `AVERAGE(Correlation_Data[Avg_Death_Rate])` - Respiratory Death Rate (per 100k)
- **Values**: Show both measures side by side

**For correlation coefficients**, you can:
- Use a Python or R script visual to calculate Pearson correlation
- Export data and calculate in Excel/Python
- Use a custom visual from AppSource that calculates correlations

#### 3. Time Series Comparison (Dual Axis Line Chart)

- **Primary Y-axis**: `SUM(vw_emissions[emissions_kt_co2e])` (left axis)
- **Secondary Y-axis**: `AVERAGE(vw_health_metrics[age_standardised_rate_per_100k])` (right axis)
- **X-axis**: `vw_emissions[year]` or `vw_health_metrics[year]` (use one, filter to match)
- **Title**: "Emissions and Respiratory Death Rate Over Time"
- **Filters**: 
  - Same country/region (use slicers to sync)
  - Year range: 2000-2021 (to match health data availability)
  - `vw_health_metrics[is_respiratory] = 1`
  - `vw_emissions[nuts_level] = 0` and `vw_health_metrics[nuts_level] = 0` (for country-level)

#### 4. Regional Comparison: Emissions vs. Health (Clustered Bar Chart)

Use the `Correlation_Data` table:

- **Axis**: `Correlation_Data[country_name]` or `Correlation_Data[nuts_label]` (top 10 - use filter)
- **Values**: 
  - `SUM(Correlation_Data[Total_Emissions])` (bar 1)
  - `AVERAGE(Correlation_Data[Avg_Death_Rate])` (bar 2)
- **Title**: "Emissions vs. Respiratory Death Rate by Region"
- **Note**: Since scales are different, consider using two separate charts or normalize values
- **Filter**: Top N = 10, by `SUM(Total_Emissions)`

#### 5. Correlation by Sector (Scatter Plot)

Create a calculated table for sector-level correlation:

```dax
Correlation_By_Sector = 
VAR EmissionsBySector = 
    SUMMARIZE(
        vw_emissions,
        vw_emissions[nuts_id],
        vw_emissions[country_name],
        vw_emissions[sector_group],
        vw_emissions[year],
        "Total_Emissions", SUM(vw_emissions[emissions_kt_co2e])
    )
VAR HealthByRegion = 
    SUMMARIZE(
        FILTER(vw_health_metrics, vw_health_metrics[is_respiratory] = 1),
        vw_health_metrics[nuts_id],
        vw_health_metrics[country_name],
        vw_health_metrics[year],
        "Avg_Death_Rate", AVERAGE(vw_health_metrics[age_standardised_rate_per_100k])
    )
RETURN
    NATURALINNERJOIN(EmissionsBySector, HealthByRegion)
```

Then create scatter plot:
- **X-axis**: `Correlation_By_Sector[Total_Emissions]`
- **Y-axis**: `Correlation_By_Sector[Avg_Death_Rate]`
- **Legend**: `Correlation_By_Sector[sector_group]`
- **Title**: "Emissions by Sector vs. Respiratory Health Outcomes"

#### 6. Lag Analysis (Line Chart)

Create a calculated table with lagged health data:

```dax
Lag_Analysis = 
VAR EmissionsData = 
    SUMMARIZE(
        vw_emissions,
        vw_emissions[nuts_id],
        vw_emissions[country_name],
        vw_emissions[year],
        "Emissions", SUM(vw_emissions[emissions_kt_co2e])
    )
VAR HealthData = 
    SUMMARIZE(
        FILTER(vw_health_metrics, vw_health_metrics[is_respiratory] = 1),
        vw_health_metrics[nuts_id],
        vw_health_metrics[country_name],
        vw_health_metrics[year],
        "Death_Rate", AVERAGE(vw_health_metrics[age_standardised_rate_per_100k])
    )
VAR HealthDataLag1 = 
    ADDCOLUMNS(
        HealthData,
        "Year_Lag1", [year] + 1,
        "Death_Rate_Lag1", [Death_Rate]
    )
RETURN
    NATURALINNERJOIN(
        EmissionsData,
        SELECTCOLUMNS(
            HealthDataLag1,
            "nuts_id", [nuts_id],
            "country_name", [country_name],
            "year", [Year_Lag1],
            "Death_Rate_Lag1", [Death_Rate_Lag1]
        )
    )
```

Then create dual-axis line chart:
- **X-axis**: `Lag_Analysis[year]`
- **Y-axis 1**: `SUM(Lag_Analysis[Emissions])` (current year)
- **Y-axis 2**: `AVERAGE(Lag_Analysis[Death_Rate_Lag1])` (health data from year+1)
- **Title**: "Emissions vs. Health Outcomes (with 1-Year Lag)"

---

## Overview Page (Page 1)

### Key Metrics Cards

1. **Total Emissions (kt CO₂e)**
   - Measure: `SUM(vw_emissions[emissions_kt_co2e])`
   - Format: Decimal, 0 decimals, show in thousands/millions
   - Filter: Apply slicer filters if needed

2. **Average Respiratory Death Rate (per 100k)**
   - Measure: `AVERAGE(vw_health_metrics[age_standardised_rate_per_100k])`
   - Filter: `vw_health_metrics[is_respiratory] = 1`
   - Format: Decimal, 1 decimal

3. **Number of Regions**
   - Measure: `DISTINCTCOUNT(vw_emissions[nuts_id])`
   - Format: Whole number

4. **Time Period Coverage**
   - Text card: "Emissions: 1990-2022 | Health: 2000-2021"

### Summary Visualizations

1. **Emissions Trend** (mini line chart)
   - Axis: `vw_emissions[year]`
   - Values: `SUM(vw_emissions[emissions_kt_co2e])`
   - Filter: `nuts_level = 0` (country level)

2. **Health Trend** (mini line chart)
   - Axis: `vw_health_metrics[year]`
   - Values: `AVERAGE(vw_health_metrics[age_standardised_rate_per_100k])`
   - Filters: `is_respiratory = 1`, `nuts_level = 0`

3. **Top 5 Countries by Emissions** (mini bar chart)
   - Axis: `vw_emissions[country_name]`
   - Values: `SUM(vw_emissions[emissions_kt_co2e])`
   - Filter: Top N = 5, `nuts_level = 0`

4. **Top 5 Countries by Respiratory Death Rate** (mini bar chart)
   - Axis: `vw_health_metrics[country_name]`
   - Values: `AVERAGE(vw_health_metrics[age_standardised_rate_per_100k])`
   - Filters: Top N = 5, `is_respiratory = 1`, `nuts_level = 0`

---

## Best Practices

### 1. Performance Optimization

- **Views are already optimized**: Pre-joined data means faster queries
- **Filter early**: Apply year/country filters using slicers (they filter at query level)
- **Limit data for testing**: Use visual-level filters to limit year ranges during development
- **Disable auto-date/time**: File → Options → Data Load → Disable "Auto date/time" (we have our own time dimension)
- **Use calculated tables sparingly**: Only create them when needed for correlations

### 2. Data Quality

- **Check for nulls**: Use `ISBLANK()` in measures
- **Handle missing data**: Use `IF()` statements in DAX
- **Validate data**: Views already handle joins, but check for missing years/regions
- **Year alignment**: Health data (2000-2021) vs Emissions (1990-2022) - use appropriate filters

### 3. Visual Design

- **Consistent color scheme**: Use theme colors across all pages
- **Clear titles**: Include units (kt CO₂e, per 100k) and data source
- **Data labels**: Show values on charts when space allows
- **Tooltips**: Add detailed tooltips with additional context (region, year, etc.)
- **Sync slicers**: Use "Sync slicers" pane to share filters across pages

### 4. DAX Measures

Create reusable measures in the **Model view**:

```dax
Total Emissions = SUM(vw_emissions[emissions_kt_co2e])

Avg Respiratory Death Rate = 
CALCULATE(
    AVERAGE(vw_health_metrics[age_standardised_rate_per_100k]),
    vw_health_metrics[is_respiratory] = 1
)

Total Hospital Discharges = 
CALCULATE(
    SUM(vw_hospital_discharges[discharge_count]),
    vw_hospital_discharges[is_respiratory] = 1
)

Avg Hospital Discharge Rate = 
CALCULATE(
    AVERAGE(vw_hospital_discharges[discharge_rate_per_100k]),
    vw_hospital_discharges[is_respiratory] = 1
)
```

**Note**: For emissions per capita, you would need population data. If you load `fact_population`, you can create:

```dax
Emissions per Capita = 
DIVIDE(
    [Total Emissions],
    SUM(fact_population[population]),
    0
)
```

### 5. Filters and Slicers

- **Sync slicers**: Use "Sync slicers" pane for cross-page filtering
- **Visual-level filters**: Apply filters directly to visuals for performance
- **Date ranges**: Use relative date filters (last 5 years, etc.)

### 6. Export and Sharing

- **Save as .pbix**: Regular Power BI Desktop file
- **Publish to Power BI Service**: For sharing (requires Power BI Pro/Premium)
- **Export data**: Right-click visual → Export data

---

## Troubleshooting

### Issue: Data types are text instead of numbers

**Solution**: 
- Views should have correct types, but if you see issues:
- Go to **Data view** → Select column → Change type in ribbon
- For numeric columns, change to **Whole Number** or **Decimal Number**

### Issue: Missing data in correlations

**Solution**: 
- Ensure year ranges match (health: 2000-2021, emissions: 1990-2022)
- Use calculated tables to join data by `nuts_id` and `year`
- Check that both views have data for the same regions

### Issue: Views showing different data than expected

**Solution**: 
- Check filters on visuals (especially `nuts_level` and `is_respiratory`)
- Verify year ranges in slicers
- Check that views are loading correctly (Data view → preview data)

### Issue: Slow performance

**Solution**:
- Use slicers to filter early (they filter at query level)
- Limit date ranges in visual-level filters during development
- Views are already optimized, but large date ranges can be slow
- Disable auto-date/time tables
- Consider using DirectQuery mode for very large datasets (File → Options → DirectQuery)

### Issue: Missing correlations

**Solution**:
- Ensure same year range for emissions and health data
- Filter to same regions (NUTS level)
- Create calculated table to join emissions and health by region+year

---

## Example Dashboard Layout

### Page 1: Overview
```
[Key Metrics Cards - 4 cards in a row]
[Emissions Trend - Line Chart]
[Health Trend - Line Chart]
[Top Countries - 2 bar charts side by side]
```

### Page 2: Emissions Analysis
```
[Slicers - Year, Country, Sector, Gas - Top row]
[Total Emissions Map - Full width]
[Emissions Trend - Line Chart - Left half]
[Emissions by Sector - Stacked Bar - Right half]
[Emissions by Gas - Pie Chart - Left half]
[Top 10 Regions Table - Right half]
```

### Page 3: Health Analysis
```
[Slicers - Year, Country, Category - Top row]
[Respiratory Death Rate Map - Full width]
[Death Rate Trend - Line Chart - Left half]
[Death Rates by Category - Stacked Bar - Right half]
[Hospital Discharge Rates - Bar Chart - Left half]
[Top 10 Regions Table - Right half]
```

### Page 4: Correlation Analysis
```
[Slicers - Year Range, Country, Region - Top row]
[Scatter Plot: Emissions vs. Death Rate - Full width]
[Time Series Comparison - Dual Axis Line - Left half]
[Regional Comparison - Clustered Bar - Right half]
[Correlation by Sector - Scatter Plot - Full width]
```

---

## Next Steps

1. **Customize**: Adjust visualizations to your needs
2. **Add calculations**: Create DAX measures for specific metrics
3. **Create bookmarks**: Save specific filter states
4. **Add drill-through**: Enable drill-through to detailed pages
5. **Publish**: Share with your team via Power BI Service

---

## Additional Resources

- **Power BI Documentation**: https://docs.microsoft.com/power-bi/
- **DAX Guide**: https://dax.guide/
- **Database Schema**: See `prod/sql/schema.sql`
- **ETL Documentation**: See `prod/README.md`

---

## Support

For issues with:
- **Database connection**: See `prod/README.md`
- **Data quality**: Check ETL pipeline logs
- **Power BI features**: Consult Power BI documentation

Happy dashboarding! 📊

