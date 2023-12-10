from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import matplotlib.pyplot as plt
import pandas as pd

spark = SparkSession.builder.appName('DistrictsAnalysis').getOrCreate()

input_path = './output.csv'

df = spark.read.csv(input_path, header=True, inferSchema=True)

selected_columns = ['Transaction_unique_identifier', 'price', 'Old/New']
df_selected = df.select(selected_columns)

results = (
    df_selected
    .groupBy('Old/New')
    .agg({'price': 'mean'})
    .orderBy('Old/New')
)

results.show()

top_n_districts = 5
districts_top = results.limit(top_n_districts)
results_pd = districts_top.toPandas()

plt.figure(figsize=(8, 6))

plt.xlabel('Tipo de Propiedad (Nueva/Antigua)')
plt.ylabel('Precio promedio')
plt.title('Comparación de Precios entre Propiedades Nuevas y Antiguas')
plt.bar(results_pd['Old/New'], results_pd['avg(price)'])

plt.show()

spark.stop()
