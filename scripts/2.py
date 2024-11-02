from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
from pyspark.sql.functions import count, desc
from pyspark.sql.functions import lit

PATH_DIR_DATASET: str = "hdfs://192.168.104.45:9000/user/amircoli/BDA2324/" + "*"
RESULT_DIR_DATASET: str = "file:///home/amircoli/BDAchallenge2324/results/4/"
RESULT_CSV_NAME: str ="r2_4"
COORDINATES: str = "[(60,-135);(30,-90)]"

sc = SparkContext.getOrCreate()
spark = SparkSession(sc)


df = spark.read.option("header", "true") \
    .option("recursiveFileLookup", "true") \
    .csv(PATH_DIR_DATASET)

filter_df= df.select("LATITUDE","LONGITUDE","TMP")\
             .where((df['LATITUDE'] >=30) & (df['LATITUDE'] <=60) & 
                    (df['LONGITUDE'] >= -135) & (df['LONGITUDE'] <= -90))


df2 = filter_df.groupBy('TMP').agg(count('*').alias('N_TMP'))

df3=df2.withColumn("Coordinates", lit(COORDINATES)).sort('N_TMP', ascending= False).limit(10)

df_order= ['Coordinates', 'TMP', 'N_TMP']

df3.select(df_order).write.options(header='True', delimiter=',', mode='overwrite').csv(RESULT_DIR_DATASET + "/" + RESULT_CSV_NAME )
