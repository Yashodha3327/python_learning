from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
spark=SparkSession.builder.master("local").appName("Covid_Data_Analysis").getOrCreate()
df=spark.read.csv("country_wise_latest.csv",header=True)
print('Before Column Renaming')
df.printSchema()
df=df.withColumnRenamed('Country/Region','Country')\
        .withColumnRenamed('Deaths / 100','Death_per_100_cases')\
        .withColumnRenamed('New Cases','New_cases')\
        .withColumnRenamed('New deaths','New_deaths')\
        .withColumnRenamed('New recovered','New_recovered')\
        .withColumnRenamed('Deaths / 100 Cases','Death_per_100_cases')\
        .withColumnRenamed('Recovered / 100 Cases','Recovered_per_100_cases')\
        .withColumnRenamed('Deaths / 100 Recovered','deaths_per_100_recovered')\
        .withColumnRenamed('Confirmed last week','Confirmed_last_week')\
        .withColumnRenamed('1 week change','1_week_change')\
        .withColumnRenamed('1 week % increase','1_week_%_increase')\
        .withColumnRenamed('WHO Region','WHO_Region')\
        .withColumn('Confirmed',df.Confirmed.cast(IntegerType()))\
        .withColumn('Deaths',df.Deaths.cast(IntegerType()))\
        .withColumn('Recovered',df.Recovered.cast(IntegerType()))\
        .withColumn('Active',df.Active.cast(IntegerType()))

print('After Column Renaming')
df.printSchema()
df.select([count(when(isnan(c) | isnull(c),c) ).alias(c) for c in df.columns]).show()
df_desc=df.groupBy('WHO_Region','Country').agg(sum('Confirmed').alias('total_confirmed_cases')).sort(desc('total_confirmed_cases'))
df_desc.show(10)
df_asc=df.groupBy('WHO_Region','Country').agg(sum('Confirmed').alias('total_confirmed_cases')).sort(asc('total_confirmed_cases'))
df_asc.show(10)


