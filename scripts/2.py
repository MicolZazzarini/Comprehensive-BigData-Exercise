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

#condizione = df['LATITUDE'].between(30, 60) & df['LONGITUDE'].between(-135, -90)
#df_filtrato = df[condizione]
#new_df = df_filtrato[["LATITUDE", "LONGITUDE", "TMP"]]
filter_df= df.select("LATITUDE","LONGITUDE","TMP")\
             .where((df['LATITUDE'] >=30) & (df['LATITUDE'] <=60) & 
                    (df['LONGITUDE'] >= -135) & (df['LONGITUDE'] <= -90))





df2 = filter_df.groupBy('TMP').agg(count('*').alias('N_TMP'))
#Inserisce una nuova colonna Coordinates, che conterra valori costanti per tutte le righe, dove vengono specificate le cordinate,
#e poi si selezionano solo le prime 10.
# E' stata inserita la libreria "from pyspark.sql.functions import lit" per utilizzare la funzione lit
df3=df2.withColumn("Coordinates", lit(COORDINATES)).sort('N_TMP', ascending= False).limit(10)
#ordino l'output
df_order= ['Coordinates', 'TMP', 'N_TMP']

df3.select(df_order).write.options(header='True', delimiter=',', mode='overwrite').csv(RESULT_DIR_DATASET + "/" + RESULT_CSV_NAME )
