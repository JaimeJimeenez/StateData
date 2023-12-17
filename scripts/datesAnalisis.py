from pyspark.sql import SparkSession
from pyspark.sql.functions import stddev, year
import matplotlib.pyplot as plt
import pandas as pd

spark = SparkSession.builder.appName("DistrictVariabilityByYear").getOrCreate()

file_path = "gs://stone-ground-400412/output.csv" 
df = spark.read.csv(file_path, header=True)

df = df.withColumn("price", df["price"].cast("double"))

selected_county = ["GREATER LONDON", "TYNE AND WEAR", "WEST MIDLANDS", "MERSEYSIDE", "GREATER MANCHESTER", "SHROPSHIRE"]
df_selected_county = df.filter(df["County"].isin(selected_county))
df_selected_county.show()

df_selected_county = df_selected_county.withColumn("Year", year("Date_of_Transfer"))

county_year_variability = df_selected_county \
    .groupBy("County", "Year") \
    .agg(stddev("price").alias("Variability"))

county_year_variability = county_year_variability.orderBy("County", "Year")

county_year_variability.show(truncate=False)

pandas_df = county_year_variability.toPandas()

plt.figure(figsize=(12, 6))
for county in selected_county:
    df_county = pandas_df[pandas_df["County"] == county]
    plt.plot(df_county["Year"], df_county["Variability"], label=county)

plt.xlabel('Año')
plt.ylabel('Desviación estándar de la variabilidad')
plt.title('Variabilidad del mercado de los condados según el año')
plt.xticks(rotation=45, ha='right')
plt.legend()
plt.tight_layout()
plt.show()

spark.stop()
