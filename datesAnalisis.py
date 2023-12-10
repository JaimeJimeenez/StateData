from pyspark.sql import SparkSession
from pyspark.sql.functions import col, year, month
import matplotlib.pyplot as plt
import pandas as pd

spark = SparkSession.builder.appName("TimeAnalysis").getOrCreate()

input_path = "./output.csv"

df = spark.read.csv(input_path, header=True, inferSchema=True)

selected_columns = ["Transaction_unique_identifier", "price", "Date_of_Transfer"]
df_selected = df.select(selected_columns)

df_selected = df_selected.withColumn("Year", year(col("Date_of_Transfer")))
df_selected = df_selected.withColumn("Month", month(col("Date_of_Transfer")))

results = (
    df_selected
    .groupBy("Month")
    .agg({"price": "mean"})
    .orderBy("Month")
)
results.show()

results_pd = results.toPandas()

plt.figure(figsize=(12, 6))
plt.xlabel("Fecha de Transferencia")
plt.ylabel("Precio Promedio")
plt.title("Tendencias Temporales por mes en Transacciones Inmobiliarias")
plt.plot(results_pd["Month"].astype(str),
         results_pd["avg(price)"])

plt.show()

spark.stop()
