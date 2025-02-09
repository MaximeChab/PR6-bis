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



def aggregateLigne(reading_path, writing_path):
    print(f"Transforming Lignes data")
    df_lignes = spark.read.parquet(reading_path)
    df_lignes=df_lignes.withColumn("c_geo_f", concat_ws(",", col("c_geo_f.lat"), col("c_geo_f.lon")))


    rows = []

    for ligne in df_lignes.collect():
        libelle_ligne = ligne["lib_ligne"]
        type_ligne = ligne["type_ligne"]
        index=1

        depart_latitude = ligne["c_geo_d"].split(",")[0]
        depart_longitude = ligne["c_geo_d"].split(",")[1]
        rows.append(Row(libelle_ligne, type_ligne, index, depart_latitude, depart_longitude))
        index += 1
        
        for point in ligne["coordinates"]:
            rows.append(Row(libelle_ligne, type_ligne, index, point[1], point[0]))
            index += 1

        arrive_latitude = ligne["c_geo_f"].split(",")[0]
        arrive_longitude = ligne["c_geo_f"].split(",")[1]
        rows.append(Row(libelle_ligne, type_ligne, index, arrive_latitude, arrive_longitude))

    df_lignes_final = spark.createDataFrame(rows,["libelle_ligne", "type_ligne", "point_number", "latitude", "longitude"])
            
    df_lignes_final.write.mode('overwrite').option("header", True).csv(writing_path)
    df_lignes_final.write.mode('overwrite').option("header", True).csv(writing_path)

    print(f"Transforming Objets-trouvés data")
    df_objets_trouves = spark.read.parquet(reading_path)
    df_objets_trouves.write.mode('overwrite').option("header", True).csv(writing_path)