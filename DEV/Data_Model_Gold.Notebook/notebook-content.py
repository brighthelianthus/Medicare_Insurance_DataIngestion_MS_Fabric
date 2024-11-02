# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "8c6482cb-5199-4440-83b7-73b83c8b25f3",
# META       "default_lakehouse_name": "medicare_LH",
# META       "default_lakehouse_workspace_id": "e57e78e1-8889-467c-a57e-834de1b7f7d8",
# META       "known_lakehouses": [
# META         {
# META           "id": "8c6482cb-5199-4440-83b7-73b83c8b25f3"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# Welcome to your new notebook
# Type here in the cell editor to add code!
from pyspark.sql.functions import col, lit, concat, to_date, row_number, count
from pyspark.sql.types import *
from delta.tables import *
from pyspark.sql.window import Window

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ##### Creating Dimension tables from the  medicare_processed_bronze table

# MARKDOWN ********************

# ##### 1. Dim_Year table creation

# CELL ********************

df = spark.read.format('delta').load('abfss://Dev_MedicareProject@onelake.dfs.fabric.microsoft.com/medicare_LH.Lakehouse/Tables/medicare_processed_bronze')


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_year = df.select(col('Year')).withColumn("Year_Key", to_date(concat(col("year"), lit('-01-01')))).distinct()



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_year.write.mode('overwrite').saveAsTable('medicare_LH.dim_year')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ##### 2. Dim_Geography table creation

# CELL ********************

window = Window.orderBy(col('Prscrbr_State_Abrvtn'), col('Prscrbr_State_FIPS'),col('Prscrbr_City'))
df_geo = df.select(col('Prscrbr_City'), col('Prscrbr_State_Abrvtn'), col('Prscrbr_State_FIPS'))
df_geo = df_geo.distinct()
df_geo = df_geo.withColumn("geo_key",row_number().over(window))
df_geo.write.mode('overwrite').format('parquet').save('Files/gold/dim_geo')


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ##### 3. Dim_Provider table creation

# CELL ********************

window = Window.orderBy(col('Prscrbr_NPI'), col('Prscrbr_Last_Org_Name'),col('Prscrbr_First_Name'), col('Prscrbr_Type'), col('Prscrbr_Type_Src'))
df_provider = df.select(col('Prscrbr_NPI'), col('Prscrbr_Last_Org_Name'), col('Prscrbr_First_Name'), col('Prscrbr_Type'), col('Prscrbr_Type_Src'))
df_provider = df_provider.distinct()

df_provider = df_provider.withColumn("provider_key",row_number().over(window))
df_provider.write.mode('overwrite').format('parquet').save('Files/gold/dim_provider')
display(df_provider)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ##### 4. DIM_Drug table creation

# CELL ********************

window = Window.orderBy(col('Brnd_Name'), col('Gnrc_Name'))
df_drug = df.select(col('Brnd_Name'), col('Gnrc_Name'))
df_drug = df_drug.distinct()
df_drug = df_drug.withColumn("drug_key",row_number().over(window))
df_drug.write.mode('overwrite').format('parquet').save('Files/gold/dim_drug')
display(df_drug)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### Creating Fact Table with required columns and FK to toher dim tables

# CELL ********************

df.createOrReplaceTempView('medicare_processed_bronze_vw')
df_drug.createOrReplaceTempView('df_drug_vw')
df_provider.createOrReplaceTempView('df_provider_vw')
df_geo.createOrReplaceTempView('df_geo_vw')
df_year.createOrReplaceTempView('df_year_vw')


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_fact = spark.sql('''
select 
 Tot_Clms,
 Tot_30day_Fills,
 Tot_Day_Suply,
 Tot_Drug_Cst,
 Tot_Benes,
 GE65_Sprsn_Flag,
 GE65_Tot_Clms,
 GE65_Tot_30day_Fills,
 GE65_Tot_Drug_Cst,
 GE65_Tot_Day_Suply,
 GE65_Bene_Sprsn_Flag,
 GE65_Tot_Benes,
 year,
 geo_key,
 provider_key,
 drug_key
from medicare_processed_bronze_vw a left join df_drug_vw b on a.Brnd_Name = b.Brnd_Name and a.Gnrc_Name = b.Gnrc_Name
left join df_provider_vw c on a.Prscrbr_NPI = c.Prscrbr_NPI and a.Prscrbr_Last_Org_Name = c.Prscrbr_Last_Org_Name and a.Prscrbr_First_Name = c.Prscrbr_First_Name and a.Prscrbr_Type = c.Prscrbr_Type and a.Prscrbr_Type_Src = c.Prscrbr_Type_Src
left join df_geo_vw f on a.Prscrbr_State_FIPS = f.Prscrbr_State_FIPS and a.Prscrbr_City = f.Prscrbr_City
''')

##df_fact.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_fact.write.mode('overwrite').format('parquet').partitionBy('year','drug_key').save('Files/gold/fact_drug_sales_by_geo_provider')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# SCD Type 1 Example with SQL by directly upserting into the parquet table using incoming dataframe

# IN EITHER CASE of incremental loading implementation, WE WILL NEED TO DO THE INCREMENTAL LOAD FOR SCD TYPE 1 IN BOTH THE PLACES - FILES AND TABLES, OR WE CAN JUST OVERWRITE IN BOTH THE PLACES . 
#SO WHICHEVER TAKES LESS TIME ( INCREMENTAL LOADING * 2 OR OVERWRITING * 2), WE SHOULD GO FOR IT


# Step 1: Load existing data from the target table into a DataFrame
existing_data_path = "Files/gold/table_name"  # Path to existing table
existing_df = spark.read.parquet(existing_data_path)

# Step 2: Create or load the incoming DataFrame (new data)
incoming_data = [
    ('John', 'Doe', '31'),      # Update record
    ('Jane', 'Smith', '25'),    # New record
    ('Alice', 'Johnson', '28')   # No change
]
columns = ['FirstName', 'LastName', 'Age']
incoming_df = spark.createDataFrame(incoming_data, columns)

# Step 3: Write the incoming DataFrame to a temporary location (if necessary)
incoming_data_path = "Files/temp/incoming_data"  # Temporary path for incoming data
incoming_df.write.mode("overwrite").parquet(incoming_data_path)

# Step 4: Perform the merge operation directly using SQL
spark.sql(f"""
MERGE INTO table_name AS existing
USING parquet.`{incoming_data_path}` AS incoming
ON existing.FirstName = incoming.FirstName AND existing.LastName = incoming.LastName
WHEN MATCHED THEN
    UPDATE SET existing.Age = incoming.Age
WHEN NOT MATCHED THEN
    INSERT (FirstName, LastName, Age) VALUES (incoming.FirstName, incoming.LastName, incoming.Age)
""")

# Step 5: Verify the results by reading the updated table
updated_df = spark.read.parquet(existing_data_path)
print("Updated DataFrame after SCD Type 1:")
##updated_df.show()



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### Saving all Tables - dimension as well as fact tables as managed tables 

# CELL ********************

df_geo.write.mode('overwrite').format('delta').saveAsTable('dim_geo')
df_drug.write.mode('overwrite').format('delta').saveAsTable('dim_drug')
df_provider.write.mode('overwrite').format('delta').saveAsTable('dim_provider')
df_fact.write.mode('overwrite').partitionBy('year','drug_key').format('delta').saveAsTable('fact_drug_sales_by_geo_provider')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_geo.write.mode('overwrite').format('delta').saveAsTable('dim_geohraphy')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
