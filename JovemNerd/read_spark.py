# %%

from pyspark.sql import SparkSession

spark = (SparkSession.builder
         .appName('Python Spark SQL example')
         .config('spark.some.config.option', 'some-value')
         .getOrCreate())

# %%

df = spark.read.json('/mnt/datalake/JovemNerd/episodes/json')
df.show()
# %%
