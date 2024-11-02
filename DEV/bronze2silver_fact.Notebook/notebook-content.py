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
# META       "default_lakehouse_workspace_id": "e57e78e1-8889-467c-a57e-834de1b7f7d8"
# META     }
# META   }
# META }

# CELL ********************

mssparkutils.fs.ls ('Files/bronze')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Welcome to your new notebook
# Type here in the cell editor to add code!

from pyspark.sql.functions import concat, lit, col, StringType

base_path = 'Files/bronze'
filename = "medicare.csv"

schema = spark.read.format('csv').option('header',True).option('delimiter',',').load(base_path+"/2022/"+filename).schema
df_all_year = spark.createDataFrame([], schema   )
df_all_year = df_all_year.withColumn("year", lit(None))


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#df_all_year.schema

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

for i in mssparkutils.fs.ls (base_path):
    
    full_path = i.path + "/" + filename
    print(full_path)
    yr =  i.name
    print(yr)
    df = spark.read.format('csv').option('header',True).option('delimiter',',').option('inferschema',True).load(full_path).withColumn("year",lit(yr))
    print(df.count())
    df_all_year = df_all_year.union(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#display(df_all_year)
print (df_all_year.count(), ' total records')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#df_all_year.columns

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.types import IntegerType, DoubleType, StringType

df_all_year = \
df_all_year.withColumn('Prscrbr_NPI',df_all_year.Prscrbr_NPI.cast(IntegerType()))\
.withColumn('Prscrbr_State_FIPS',df_all_year.Prscrbr_State_FIPS.cast(IntegerType()))\
.withColumn('Tot_Clms',df_all_year.Tot_Clms.cast(IntegerType()))\
.withColumn('Tot_30day_Fills',df_all_year.Tot_30day_Fills.cast(DoubleType()))\
.withColumn('Tot_Day_Suply',df_all_year.Tot_Day_Suply.cast(IntegerType()))\
.withColumn('Tot_Drug_Cst',df_all_year.Tot_Drug_Cst.cast(DoubleType()))\
.withColumn('Tot_Benes',df_all_year.Tot_Benes.cast(IntegerType()))\
.withColumn('GE65_Tot_Clms',df_all_year.GE65_Tot_Clms.cast(IntegerType()))\
.withColumn('GE65_Tot_30day_Fills',df_all_year.GE65_Tot_30day_Fills.cast(DoubleType()))\
.withColumn('GE65_Tot_Drug_Cst',df_all_year.GE65_Tot_Drug_Cst.cast(DoubleType()))\
.withColumn('GE65_Tot_Day_Suply',df_all_year.GE65_Tot_Day_Suply.cast(IntegerType()))\
.withColumn('GE65_Tot_Benes',df_all_year.GE65_Tot_Benes.cast(IntegerType()))

df_all_year.select(col("year").cast('int').alias("year"))




# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#df_all_year.printSchema()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(df_all_year)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_all_year.createOrReplaceTempView("fact")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# 
# ##### surrogate key or a composite primary key can be defined for each row in the table using the columns 
# Prscrbr_NPI, Prscrbr_Last_Org_Name,Prscrbr_First_Name, Prscrbr_City, Prscrbr_State_FIPS,Prscrbr_Type, Prscrbr_Type_Src, Brnd_Name,Gnrc_Name ,GE65_Bene_Sprsn_Flag, GE65_Sprsn_Flag, year

# CELL ********************

from pyspark.sql.functions import coalesce

df_with_composite_key = df_all_year.withColumn(
    'row_uk',
    concat(col('Prscrbr_NPI'), 
    coalesce(col('Prscrbr_Last_Org_Name'), lit('')),
    coalesce(col('Prscrbr_First_Name'), lit('')),
    col('Prscrbr_City'), 
    col('Prscrbr_State_FIPS'),
    col('Prscrbr_Type'),
    col('Prscrbr_Type_Src'), 
    col('Brnd_Name'),
    col('Gnrc_Name') ,
    coalesce(col('GE65_Bene_Sprsn_Flag'), lit('')),
    coalesce(col('GE65_Sprsn_Flag'),lit('')),
    col( 'year'))
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_with_composite_key.columns

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(df_with_composite_key)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

## INCREMENTAL LOAD SCD TYPE 1 AS PER PUR USE CASE

from pyspark.sql.utils import AnalysisException

try:
    table_name = 'medicare_LH.medicare_processed_bronze'

    df_with_composite_key.write.partitionBy('year','Gnrc_Name','Brnd_Name').format("delta").saveAsTable(table_name)

except AnalysisException as e:   ### SCD type 1

    print ("Table already exists")

    df_with_composite_key.createOrReplaceTempView("view_df")

    spark.sql(f""" MERGE INTO {table_name} target_table
                USING view_df source_view
                on source_view.row_uk = target_table.row_uk

                WHEN MATCHED AND    ---- SOME INFORMATION FOR EXISTING NEWS URL GOT UPDATED
                (
                source_view.Tot_Clms <> target_table.Tot_Clms OR
                source_view.Tot_30day_Fills <> target_table.Tot_30day_Fills OR
                source_view.Tot_Day_Suply <> target_table.Tot_Day_Suply OR
                source_view.Tot_Drug_Cst <> target_table.Tot_Drug_Cst OR
                source_view.Tot_Benes <> target_table.Tot_Benes OR  
                source_view.GE65_Tot_Clms <> target_table.GE65_Tot_Clms OR
                source_view.GE65_Tot_30day_Fills <> target_table.Tot_30day_Fills OR
                source_view.GE65_Tot_Drug_Cst <> target_table.GE65_Tot_Drug_Cst OR
                source_view.GE65_Tot_Day_Suply <> target_table.GE65_Tot_Day_Suply OR
                source_view.GE65_Tot_Benes <> target_table.GE65_Tot_Benes
                )
                THEN UPDATE SET *

                WHEN NOT MATCHED   ---- COMPLETELY NEW NEWS URL RECEIVED
                THEN INSERT *
                                    ----- MERGE SOES NOT DO ANYTHING IF URL RECORD IS PRESENT AND THERE IS NO CHANGE
                """)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC  select * from medicare_LH.medicare_processed_bronze limit 200

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }
