from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.ml import PipelineModel
from pyspark.ml.feature import VectorAssembler
from pyspark.sql.types import StructType, StructField, DoubleType

from pyspark import SparkConf, SparkContext




conf = SparkConf().set("spark.ui.port", "4041")\
                  .set("spark.network.timeout", "800s")


sc = SparkContext(conf=conf)

# Adjust the logging level (e.g., INFO, WARN, ERROR, DEBUG)
sc.setLogLevel("ERROR")  # This will change the log level to INFO


# Initialiser Spark Session
spark = SparkSession.builder \
    .appName("Rain Prediction Streaming") \
    .config("spark.jars", "C:/spark/spark-3.4.4-bin-hadoop3/jars/spark-sql-kafka-0-10_2.13-3.4.4.jar,C:/spark/spark-3.4.4-bin-hadoop3/jars/kafka-clients-2.8.0.jar") \
    .getOrCreate()


# Schéma des données Kafka entrantes
schema = StructType([
    StructField("MinTemp", DoubleType()),
    StructField("MaxTemp", DoubleType()),
    StructField("Rainfall", DoubleType()),
    StructField("WindGustSpeed", DoubleType()),
    StructField("Humidity3pm", DoubleType()),
    StructField("Pressure3pm", DoubleType()),
    StructField("Cloud9am", DoubleType()),
    StructField("Temp3pm", DoubleType())
])

# Charger le modèle ML sauvegardé
model = PipelineModel.load("C:/Users/pc/rain-prediction/models/rain_prediction_model")

# Lire le flux depuis Kafka
kafka_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "rain-prediction") \
    .load()

# Décoder et parser les données Kafka
parsed_stream = kafka_stream.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

# Assembler les features pour le modèle
feature_columns = ["MinTemp", "MaxTemp", "Rainfall", "WindGustSpeed", "Humidity3pm", "Pressure3pm", "Cloud9am", "Temp3pm"]
assembler = VectorAssembler(inputCols=feature_columns, outputCol="features")
processed_stream = assembler.transform(parsed_stream)

# Appliquer le modèle pour faire des prédictions
predictions = model.transform(processed_stream)

# Afficher les résultats en temps réel
query = predictions.select("prediction") \
    .writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", "false") \
    .start()

query.awaitTermination()
