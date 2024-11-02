from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
from pyspark.sql.functions import count, asc, input_file_name, split, col

ROOT_DIR_DATASET: str = "/BDA2324/"
PATH_DIR_DATASET: str = "hdfs://192.168.104.45:9000/user/amircoli/BDA2324/" + "*"
RESULT_DIR_DATASET: str = "file:///home/amircoli/BDAchallenge2324/results/4/"
RESULT_CSV_NAME: str ="r1_4"

sc = SparkContext.getOrCreate()
spark = SparkSession(sc)

df = spark.read.option("header", "true") \
    .option("recursiveFileLookup", "true") \
    .csv(PATH_DIR_DATASET) \
    .withColumn("Anno",            split( split( input_file_name(), ROOT_DIR_DATASET)[1], "/")[0] ) \
    .withColumn("Stazione", split( split( split( input_file_name(), ROOT_DIR_DATASET)[1], "/")[1], ".csv")[0])

counts = df.groupBy("Stazione", "Anno").agg(count('*').alias('Numero_misurazioni'))
df2=counts.sort('Numero_misurazioni', ascending = False)
df_order=['Anno', 'Stazione', 'Numero_misurazioni']
df2.select(df_order).write.options(header='True', delimiter=',', mode='overwrite').csv(RESULT_DIR_DATASET + "/" + RESULT_CSV_NAME )
