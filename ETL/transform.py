from pyspark.sql import SparkSession
from pyspark.sql.functions import col


spark = SparkSession.builder.master('local[*]').appName('EC').getOrCreate()

def transformData(reading_path, writing_path) :

    gares_schema = "libelle STRING, geo_point_2d STRUCT<lon:DOUBLE, lat:DOUBLE>, departemen STRING, fret STRING, voyageurs STRING"
    
    df = spark.read.json(reading_path)

    df_gares = spark.read.schema(gares_schema).json(reading_path)
    df_gares.printSchema()
    df_gares.show(5)

    df_gares.createOrReplaceTempView("gares")

    df_gares_unique  = spark.sql("select libelle, geo_point_2d, departemen, fret, voyageurs, count(*) as gares_nbr from gares group by libelle, geo_point_2d, departemen, fret, voyageurs").where("gares_nbr<2").orderBy("libelle")
    df_gares_multiple = spark.sql("select libelle, geo_point_2d,departemen, fret, voyageurs, count(*) as gares_nbr from gares group by libelle, geo_point_2d, departemen, fret, voyageurs").where("gares_nbr>1").orderBy("libelle")
    df_locations = df_gares_unique.union(df_gares_multiple).select("libelle", "geo_point_2d", "departemen", "fret", "voyageurs")

    df_locations = df_locations.replace({"N":"0"}, subset=["fret", "voyageurs"])
    df_locations = df_locations.replace({"O":"1"}, subset=["fret", "voyageurs"])

    df_locations = df_locations.withColumn("fret", col("fret").cast("boolean")).withColumn("voyageurs", col("voyageurs").cast("boolean"))

    df_locations.write.mode('overwrite').parquet(writing_path)
    print(f"data saved to {writing_path}")