# Power BI Connection Guide

This guide explains how to connect Power BI Desktop to the SQLite data warehouse.

## Prerequisites

- Power BI Desktop installed
- SQLite database created (`air_health_eu.db`)
- Data loaded into the database

## Connection Steps

### Step 1: Open Power BI Desktop

1. Launch Power BI Desktop
2. You'll see the welcome screen or a blank report

### Step 2: Connect to SQLite Database

1. Click **Get Data** (or Home → Get Data)
2. In the search box, type "SQLite" or scroll to find it
3. Select **SQLite database**
4. Click **Connect**

### Step 3: Browse to Database File

1. Navigate to: `prod/data/warehouse/air_health_eu.db`
2. Select the file
3. Click **Open**

### Step 4: Select Tables

1. You'll see the **Navigator** window
2. Check the tables you want to import:
   - ✅ `fact_climate_health` (fact table)
   - ✅ `dim_region` (dimension)
   - ✅ `dim_time` (dimension)
   - ✅ `dim_country` (dimension)
   - ✅ `vw_climate_health` (view - optional, but convenient)

3. Choose **Data connectivity mode**:
   - **Import**: Data is loaded into Power BI (recommended for small datasets)
   - **DirectQuery**: Queries database directly (for large datasets or real-time)

4. Click **Load**

### Step 5: View Data Model

1. Click the **Model** icon (bottom left, looks like a diagram)
2. You'll see your star schema:
   - Fact table in center
   - Dimension tables around it
   - Relationships shown as lines

3. Power BI should auto-detect relationships based on foreign keys
4. If relationships aren't detected, you can create them manually:
   - Drag from `fact_climate_health.region_key` to `dim_region.region_key`
   - Set cardinality: Many-to-One
   - Set filter direction: Both (or Single, depending on your needs)

## Building Visualizations

### Example: Correlation Scatter Plot

1. Click **Report** view
2. Add a scatter chart
3. Drag measures:
   - X-axis: `total_emissions_kt` (from fact table)
   - Y-axis: `cod_copd_rate` (from fact table)
   - Color: `country_name` (from dim_country)
   - Size: `population` (from fact table)

4. Add trend line: Right-click chart → Add trend line

### Example: Map Visualization

1. Add a map visual
2. Location: `nuts_id` or `nuts_label` (from dim_region)
3. Color: Any measure (e.g., `total_emissions_kt`)
4. Tooltip: Additional measures

### Example: Time Series

1. Add a line chart
2. X-axis: `year` (from dim_time)
3. Y-axis: Any measure (e.g., `total_emissions_kt`)
4. Legend: `country_name` or `nuts_label`

## Calculated Fields for Correlation

### Correlation Coefficient

```dax
Correlation = 
CORREL(
    fact_climate_health[total_emissions_kt],
    fact_climate_health[cod_copd_rate]
)
```

### R-Squared

```dax
R_Squared = 
POWER(
    CORREL(
        fact_climate_health[total_emissions_kt],
        fact_climate_health[cod_copd_rate]
    ),
    2
)
```

### Correlation by Country

```dax
Correlation by Country = 
CALCULATE(
    CORREL(
        fact_climate_health[total_emissions_kt],
        fact_climate_health[cod_copd_rate]
    ),
    ALLEXCEPT(
        fact_climate_health,
        dim_country[country_name]
    )
)
```

## Tips

1. **Use the View**: The `vw_climate_health` view combines all dimensions and facts, making it easier to build visualizations without joins

2. **Model View**: Organize your tables in Model view for better visualization:
   - Place fact table in center
   - Dimension tables around it
   - Color-code by table type

3. **Performance**: For small datasets (< 1M rows), Import mode is faster. For larger datasets, consider DirectQuery.

4. **Refresh Data**: When you update the SQLite database, refresh in Power BI:
   - Home → Refresh
   - Or: Right-click dataset → Refresh

5. **Hide Unused Columns**: In Model view, right-click columns you don't need → Hide in report view (keeps model cleaner)

## Troubleshooting

### "Cannot connect to database"

- Verify database file exists at the path
- Check file permissions
- Ensure database is not locked by another process

### "Relationships not detected"

- Manually create relationships in Model view
- Verify foreign keys exist in SQLite schema
- Check that key columns have matching data types

### "Data type errors"

- SQLite uses type affinity (not strict types)
- Power BI may infer types differently
- Use Power Query Editor to change data types if needed

## Next Steps

- Build dashboards with multiple visualizations
- Create calculated fields for advanced analysis
- Set up parameters for dynamic metric selection
- Publish to Power BI Service (if you have a license)

