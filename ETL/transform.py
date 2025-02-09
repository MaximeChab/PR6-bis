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

def transformLignes(reading_path, writing_path) :
    print(f"Cleaning Lignes data")
    lignes_schema = "lib_ligne STRING, type_ligne STRING, c_geo_d STRING, c_geo_f STRUCT<lon:DOUBLE, lat:DOUBLE>, geo_shape STRUCT<type:STRING, geometry:STRUCT<type:STRING,coordinates:ARRAY<ARRAY<STRING>>>>"

    df_lignes = spark.read.schema(lignes_schema).json(f"hdfs://localhost:9000/{reading_path}/lignes.json")

    df_lignes = df_lignes.select("lib_ligne", "geo_shape.geometry.coordinates", "type_ligne", "c_geo_d", "c_geo_f")
    df_lignes.write.mode('overwrite').parquet(f"hdfs://localhost:9000/{writing_path}/lignes_shapes.parquet")


    print(f"Cleaning Objets-trouves data")
    lignes_schema = "date STRING, gc_obo_date_heure_restitution_c STRING, gc_obo_gare_origine_r_name STRING, gc_obo_nature_c STRING, gc_obo_type_c STRING"

    df_objets_trouves = spark.read.schema(lignes_schema).json(f"hdfs://localhost:9000/{reading_path}/objets-trouves.json")

    df_objets_trouves = df_objets_trouves.select("date", "gc_obo_date_heure_restitution_c", "gc_obo_gare_origine_r_name", "gc_obo_nature_c", "gc_obo_type_c")
    df_objets_trouves.write.mode('overwrite').parquet(f"hdfs://localhost:9000/{writing_path}/objets-trouves.parquet")