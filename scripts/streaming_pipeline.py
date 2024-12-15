# streaming_pipeline.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.ml import PipelineModel
from pyspark.ml.feature import VectorAssembler
from pyspark.sql.types import StructType, StructField, DoubleType, StringType

# Initialiser Spark
spark = SparkSession.builder.appName("Rain Prediction Streaming")\
                            .config("spark.ui.port", "4050") \
                            .getOrCreate()

# Schéma des données entrantes
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

# Charger le modèle
model = PipelineModel.load("notebooks/models/rain_prediction_model")

# Connecter Kafka
kafka_stream = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "rain-prediction") \
    .load()

# Parser les données
parsed_stream = kafka_stream.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

# Assembler les features
feature_columns = ["MinTemp", "MaxTemp", "Rainfall", "WindGustSpeed", "Humidity3pm", "Pressure3pm", "Cloud9am", "Temp3pm"]
assembler = VectorAssembler(inputCols=feature_columns, outputCol="features")
processed_stream = assembler.transform(parsed_stream)

# Prédire avec le modèle
predictions = model.transform(processed_stream)

# Afficher les prédictions
query = predictions.select("prediction") \
    .writeStream.outputMode("append") \
    .format("console") \
    .start()

query.awaitTermination()
 
