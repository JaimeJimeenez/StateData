from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, year
import matplotlib.pyplot as plt
import pandas as pd

spark = SparkSession.builder.appName("DistrictAveragePriceByYear").getOrCreate()

file_path = "gs://stone-ground-400412/output.csv" 
df = spark.read.csv(file_path, header=True)

df = df.withColumn("price", df["price"].cast("double"))

selected_counties = ["GREATER LONDON", "TYNE AND WEAR", "WEST MIDLANDS", "MERSEYSIDE", "GREATER MANCHESTER", "SHROPSHIRE"]
df_selected_counties = df.filter(df["County"].isin(selected_counties))
df_selected_counties.show()

df_selected_counties = df_selected_counties.withColumn("Year", year("Date_of_Transfer"))

county_year_average_price = df_selected_counties \
    .groupBy("County", "Year") \
    .agg(avg("price").alias("AveragePrice"))

county_year_average_price = county_year_average_price.orderBy("County", "Year")

county_year_average_price.show(truncate=False)

pandas_df = county_year_average_price.toPandas()

plt.figure(figsize=(12, 6))
for county in selected_counties:
    df_county = pandas_df[pandas_df["County"] == county]
    plt.plot(df_county["Year"], df_county["AveragePrice"], label=county)

plt.xlabel('Año')
plt.ylabel('Precio Promedio')
plt.title('Precio Promedio del Mercado por Condado y Año')
plt.xticks(rotation=45, ha='right')
plt.legend()
plt.tight_layout()
plt.show()

spark.stop()


