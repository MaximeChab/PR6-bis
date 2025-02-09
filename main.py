from ETL.extract import extractAndSave
from ETL.transform import transformData
from ETL.analyse import analyse

api_url = "https://ressources.data.sncf.com/api/explore/v2.1/catalog/datasets/liste-des-gares/records?limit=-1"
api_url_lignes =  "https://ressources.data.sncf.com/api/explore/v2.1/catalog/datasets/lignes-par-type/records?limit=-1"
hdfs_file_path = "hdfs://localhost:9000/user/testbis/test.json"
hdfs_clean_path = "hdfs://localhost:9000/user/testbis/clean.json"
hdfs_analyse_path = "hdfs://localhost:9000/user/testbis/analyse.json"
local_file_path = "/tmp/data.json"


if __name__ == "__main__" :

    extractAndSave(api_url,hdfs_file_path, local_file_path)
    print(f"Extracted and saved data from {api_url} in {hdfs_file_path}")
    transformData(hdfs_file_path, hdfs_clean_path)
    analyse(hdfs_clean_path, hdfs_analyse_path)
