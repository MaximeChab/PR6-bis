from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import col, when, concat_ws, count

spark = SparkSession.builder.master('local[*]').appName('EC').getOrCreate()

def analyse(reading_path, writing_path) :
    print("transforming gares data")

    df_gares = spark.read.parquet(reading_path)

    df_gares = df_gares.withColumn("latitude", col("geo_point_2d.lat"))
    df_gares = df_gares.withColumn("longitude", col("geo_point_2d.lon"))
    df_gares = df_gares.drop("geo_point_2d")

    df_gares = df_gares.withColumn("type", when((col("fret")==True) & (col("voyageurs")==True), "Fret et voyageurs")
                                   .when(col("fret")==True, "Fret")
                                   .when(col("voyageurs")==True, "Voyageurs")
                                   .otherwise("Inconnu"))
    df_gares.show(10)
    df_gares.write.mode('overwrite').option("header", True).csv(writing_path)

    df_count_type = df_gares.groupBy("type").agg(count("*").alias("type"))
    df_count_type.show()



    print("end of the program")