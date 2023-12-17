from pyspark.sql import SparkSession
from pyspark.sql.functions import max, min, count
import matplotlib.pyplot as plt
import pandas as pd

spark = SparkSession.builder.appName("MarketVariabilityAnalysis").getOrCreate()

file_path = "gs://stone-ground-400412/output.csv"
df = spark.read.csv(file_path, header=True)

df = df.withColumn("price", df["price"].cast("double"))

district_counts = df.groupBy("County").agg(count("price").alias("Count"))
valid_districts = district_counts.filter("Count > 1").select("County")

df = df.join(valid_districts, "County", "inner")

district_variability = df.groupBy("County").agg((max("price") - min("price")).alias("Variation"))
district_variability = district_variability.orderBy("Variation", ascending=False)

district_variability.show(truncate=False)

top_n_districts = 6
districts_top = district_variability.limit(top_n_districts)
pandas_df = districts_top.toPandas()

plt.figure(figsize=(12, 6))
plt.bar(pandas_df["County"], pandas_df["Variation"], color='blue')
plt.xlabel('Condado')
plt.ylabel('Variación (Max - min)')
plt.title('Variación del mercado de los condados según la variación de los precios')
plt.xticks(rotation=45, ha='right')
plt.tight_layout()

plt.show()

spark.stop()
