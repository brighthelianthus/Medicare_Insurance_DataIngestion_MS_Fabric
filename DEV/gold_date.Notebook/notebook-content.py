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
from pyspark.sql.functions import *
from pyspark.sql.types import *

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC create table if not exists medicare_LH.gold_date
# MAGIC (
# MAGIC 
# MAGIC     BusinessDate date,
# MAGIC     Business_Year int,
# MAGIC     Business_Month int,
# MAGIC     Business_Quarter int,
# MAGIC     Business_Week int
# MAGIC ) using DELTA
# MAGIC PARTITIONED BY (Business_Year)

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

start_date = '2019-01-01'
end_date = '2022-12-31'

date_diff = spark.sql("select date_diff('{}','{}')".format(end_date,start_date)).first()[0]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

date_diff

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

id_data = spark.range(0,date_diff)
id_data.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

date_data = id_data.selectExpr("date_add('{}', cast(id as int)) as BusinessDate".format(start_date))
date_data.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

date_data.createOrReplaceTempView("ViewDate")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC insert into medicare_LH.gold_date (
# MAGIC select * , 
# MAGIC Year(BusinessDate) as Business_Year,
# MAGIC  Month(BusinessDate) as Business_Month,
# MAGIC  Quarter(BusinessDate) as Business_Quarter,
# MAGIC  weekday(BusinessDate) as Business_week
# MAGIC  from ViewDate)

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC select * from medicare_LH.gold_date 

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }
