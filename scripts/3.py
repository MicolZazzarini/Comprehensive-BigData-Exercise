from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
from pyspark.sql.functions import count, input_file_name, split, col

ROOT_DIR_DATASET: str = "/BDA2324/"
PATH_DIR_DATASET: str = "hdfs://192.168.104.45:9000/user/amircoli/BDA2324/" + "*"
RESULT_DIR_DATASET: str = "file:///home/amircoli/BDAchallenge2324/results/4/"
RESULT_CSV_NAME: str ="r3_4"

sc = SparkContext.getOrCreate()
spark = SparkSession(sc)


df = spark.read.option("header", "true") \
    .option("recursiveFileLookup", "true") \
    .csv(PATH_DIR_DATASET) \
    .withColumn("Stazione", split( split( split( input_file_name(), ROOT_DIR_DATASET)[1], "/")[1], ".csv")[0]) \
    .withColumn("Velocita_vento", split(col('WND'),",")[1])

result = df.select("Stazione", "Velocita_vento")
result.show()

counts = df.groupBy("Stazione", "Velocita_vento").agg(count('*').alias('Occorrenze_velocita'))
sorted_counts = counts.sort('Occorrenze_velocita', ascending = False)
sorted_counts.show(1, False)

first_row = sorted_counts.limit(1)

df_order= ['Stazione', 'Velocita_vento', 'Occorrenze_velocita']
first_row.select(df_order).write.options(header='True', delimiter=',', mode='overwrite').csv(RESULT_DIR_DATASET + "/" + RESULT_CSV_NAME )
