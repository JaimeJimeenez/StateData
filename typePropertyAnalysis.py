from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import matplotlib.pyplot as plt
import pandas as pd

spark = SparkSession.builder.appName("AnalsisTiposPropiedades").getOrCreate()

input_path = "./output.csv"

df = spark.read.csv(input_path, header=True, inferSchema=True)

columns_selected = ["Transaction_unique_identifier", "price", "Property_Type"]
df_selected = df.select(columns_selected)

results = (
    df_selected
    .groupBy("Property_Type")
    .agg({"price": "mean"})
    .orderBy("Property_Type")
)

results.show()

results_pd = results.toPandas()
plt.figure(figsize=(8, 6))

plt.xlabel("Tipo de Propiedad")
plt.ylabel("Precio Promedio")
plt.title("Variación de Precios entre Propiedades Residenciales y Comerciales")
plt.bar(results_pd["Property_Type"], results_pd['avg(price)'])

plt.show()

spark.stop()
