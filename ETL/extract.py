import requests
import json
from pyspark.sql import SparkSession

spark = SparkSession.builder.master('local[*]').appName('EC').getOrCreate()

def extractAndSave(api_url, hdfs_file_path, local_file_path) :
    data = []
    response = requests.get(api_url).json()
    if "results" in response:
        data += response["results"]
        offset_number = response["total_count"]
        current_offset = 100
        while current_offset < offset_number and current_offset < 9900:
            response = requests.get(api_url + f"&offset={current_offset}").json()
            data += response["results"]
            current_offset += 10 

        with open(local_file_path, 'w', encoding='utf-8') as f:
            json.dump(data,f,ensure_ascii=False, indent=4)

    df = spark.read.json(local_file_path, multiLine = True)
    df.write.mode('overwrite').json(hdfs_file_path)
    print(f"raw data saved to {hdfs_file_path}")
