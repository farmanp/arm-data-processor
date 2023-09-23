from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
import pdfplumber
import boto3
from io import BytesIO
from dotenv import load_dotenv
import os
import pymongo
import re
from pyspark.sql import SparkSession

load_dotenv()
MONGODB_HOST = os.getenv("MONGODB_HOST")
MONGODB_PORT = os.getenv("MONGODB_PORT")
MONGODB_NAME = os.getenv("MONGODB_NAME")
MONGO_URI = f"mongodb://{MONGODB_HOST}:{MONGODB_PORT}/{MONGODB_NAME}"
raw_product_manuals_collection = pymongo.MongoClient(MONGO_URI)[MONGODB_NAME][
    "raw_product_manuals"
]

raw_product_manuals = raw_product_manuals_collection.find()

schema = StructType(
    [
        StructField("_id", StringType(), True),
        StructField("text", StringType(), True),
        StructField("page_number", IntegerType(), True),
        # Add other fields as needed
    ]
)


def clean_text(text):
    text = text.replace("\n", " ")
    text = re.sub(r"\s+", " ", text)
    return text


def clean_raw_product_manuals(raw_product_manuals):
    for raw_product_manual in raw_product_manuals:
        raw_product_manual["_id"] = str(
            raw_product_manual["_id"]
        )  # Convert ObjectId to string
        raw_product_manual["text"] = clean_text(raw_product_manual["text"])
        raw_product_manual["page_number"] = int(
            raw_product_manual["page_number"]
        )  # Convert to standard integer
        yield raw_product_manual


if __name__ == "__main__":
    spark = (
        SparkSession.builder.appName("CleanProductManualsProcessing")
        .config(
            "spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:3.0.1"
        )
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("INFO")

    clean_product_manuals = clean_raw_product_manuals(raw_product_manuals)

    clean_product_manuals_df = spark.createDataFrame(
        clean_product_manuals, schema=schema
    )

    clean_product_manuals_df.write.format("mongo").option("uri", MONGO_URI).option(
        "database", MONGODB_NAME
    ).option("collection", "clean_product_manuals").mode("append").save()
