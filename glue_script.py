import sys
import re
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col, from_json, udf, concat_ws, regexp_replace, trim
from pyspark.sql.types import StructType, StructField, StringType, FloatType

# Initialize Glue and Spark contexts
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init("KafkaToS3CleanEnglishNewsJob", {})

# Define JSON schema of Kafka messages
article_schema = StructType([
    StructField("source", StructType([
        StructField("id", StringType(), True),
        StructField("name", StringType(), True)
    ]), True),
    StructField("author", StringType(), True),
    StructField("title", StringType(), True),
    StructField("description", StringType(), True),
    StructField("url", StringType(), True),
    StructField("urlToImage", StringType(), True),
    StructField("publishedAt", StringType(), True),
    StructField("content", StringType(), True)
])

# Read from Kafka
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "13.204.47.102:9092") \
    .option("subscribe", "news-topic") \
    .option("startingOffsets", "latest") \
    .load()

# Convert Kafka value to string
df_str = df.selectExpr("CAST(value AS STRING) AS json_str")

# Parse JSON
parsed_df = df_str.select(from_json(col("json_str"), article_schema).alias("data"))

# Flatten structure
flat_df = parsed_df.select(
    col("data.source.id").alias("source_id"),
    col("data.source.name").alias("source_name"),
    col("data.author"),
    col("data.title"),
    col("data.description"),
    col("data.url"),
    col("data.urlToImage"),
    col("data.publishedAt"),
    col("data.content")
)

# Drop rows with missing required fields
clean_df = flat_df.na.drop(subset=["title", "url", "publishedAt"])

# Remove unwanted characters and trim whitespace
for field in ["title", "description", "content"]:
    clean_df = clean_df.withColumn(field, regexp_replace(col(field), r'[^a-zA-Z0-9\s.,\'\"!?()-]', ''))
    clean_df = clean_df.withColumn(field, trim(col(field)))

# Filter English-like rows (basic heuristic: title must contain mostly A-Z or common English punctuation)
english_df = clean_df.filter(col("title").rlike("^[a-zA-Z0-9\\s.,'\"!?()-]{10,}$"))

# Define sentiment UDF
def get_sentiment(text):
    if not text:
        return 0.0
    text_lower = text.lower()
    positive_words = [
        'good', 'great', 'excellent', 'amazing', 'wonderful', 'fantastic',
        'positive', 'success', 'win', 'victory', 'happy', 'joy', 'love',
        'best', 'perfect', 'brilliant', 'outstanding', 'impressive',
        'breakthrough', 'achievement', 'progress', 'growth', 'boost'
    ]
    negative_words = [
        'bad', 'terrible', 'awful', 'horrible', 'worst', 'fail', 'failure',
        'negative', 'loss', 'lose', 'sad', 'angry', 'hate', 'crisis',
        'problem', 'issue', 'concern', 'worry', 'fear', 'threat',
        'decline', 'drop', 'fall', 'crash', 'disaster', 'emergency'
    ]
    pos_count = sum(1 for word in positive_words if word in text_lower)
    neg_count = sum(1 for word in negative_words if word in text_lower)
    if pos_count == 0 and neg_count == 0:
        return 0.0
    return max(-1.0, min(1.0, (pos_count - neg_count) / (pos_count + neg_count)))

sentiment_udf = udf(get_sentiment, FloatType())

# Add sentiment input and score
with_text_df = english_df.withColumn(
    "text_for_sentiment", concat_ws(". ", col("title"), col("description"))
)
with_sentiment_df = with_text_df.withColumn(
    "sentiment_score", sentiment_udf(col("text_for_sentiment"))
)

# Select final cleaned and relevant fields for output
final_df = with_sentiment_df.select(
    "source_id", "source_name", "author", "title", "description",
    "url", "urlToImage", "publishedAt", "sentiment_score"
)

# Write as pretty JSON (each record separate but clean)
query = final_df.writeStream \
    .format("json") \
    .option("path", "s3://kafkaaabucket/kafka_folder/") \
    .option("checkpointLocation", "s3://kafkaaabucket/kafka_folder/") \
    .outputMode("append") \
    .start()

query.awaitTermination()
job.commit()
