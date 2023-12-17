from pyspark.sql import SparkSession
from pyspark.sql.functions import stddev, year
import matplotlib.pyplot as plt
import pandas as pd

spark = SparkSession.builder.appName("DistrictVariabilityByYear").getOrCreate()

file_path = "gs://stone-ground-400412/output.csv" 
df = spark.read.csv(file_path, header=True)

df = df.withColumn("price", df["price"].cast("double"))

selected_districts = ["GREATER LONDON", "TYNE AND WEAR", "WEST MIDLANDS", "MERSEYSIDE", "GREATER MANCHESTER", "SHROPSHIRE"]
df_selected_districts = df.filter(df["County"].isin(selected_districts))
df_selected_districts.show()

district_year_variability = df_selected_districts.groupBy("County", "Property_Type").agg(stddev("price").alias("Variability"))

district_year_variability = district_year_variability.orderBy("County", "Property_Type")

district_year_variability.show(truncate=False)

pandas_df = district_year_variability.toPandas()

plt.figure(figsize=(12, 6))
for district in selected_districts:
    df_district = pandas_df[pandas_df["County"] == district]
    plt.plot(df_district["Property_Type"], df_district["Variability"], label=district)

plt.xlabel('Tipo de propiedad')
plt.ylabel('Desviación estándar según la variabilidad')
plt.title('Variacón del mercado de los condados según el tipo de propiedad')
plt.xticks(rotation=45, ha='right')
plt.legend()
plt.tight_layout()
plt.show()

spark.stop()
