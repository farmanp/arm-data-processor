from pyspark.sql import SparkSession
import pdfplumber
import boto3
from io import BytesIO
from dotenv import load_dotenv
import os
import pymongo

load_dotenv()
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT")
MINIO_PORT = os.getenv("MINIO_PORT")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY")
MINIO_BUCKET_NAME = os.getenv("MINIO_BUCKET_NAME")
MONGODB_HOST = os.getenv("MONGODB_HOST")
MONGODB_PORT = os.getenv("MONGODB_PORT")
MONGODB_NAME = os.getenv("MONGODB_NAME")
MONGO_URI = f"mongodb://{MONGODB_HOST}:{MONGODB_PORT}/{MONGODB_NAME}"


def process_pdf_from_minio(pdf_path):
    # Reinitialize the s3_client inside the function
    s3_client = boto3.client(
        "s3",
        endpoint_url=f"http://{MINIO_ENDPOINT}:{MINIO_PORT}",
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
    )

    pdf_object = s3_client.get_object(Bucket=MINIO_BUCKET_NAME, Key=pdf_path)
    pdf_content = pdf_object["Body"].read()

    with pdfplumber.open(BytesIO(pdf_content)) as pdf:
        for i, page in enumerate(pdf.pages):
            yield (pdf_path, i, page.extract_text())


if __name__ == "__main__":
    # Create a Spark session
    spark = (
        SparkSession.builder.appName("PDFProcessing")
        .config(
            "spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:3.0.1"
        )
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("INFO")

    # Fetch list of objects in the bucket
    s3_client = boto3.client(
        "s3",
        endpoint_url=f"http://{MINIO_ENDPOINT}:{MINIO_PORT}",
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
    )
    objects_in_bucket = s3_client.list_objects(Bucket=MINIO_BUCKET_NAME)["Contents"]

    # Filter out the PDF files from the list
    pdf_files = [obj["Key"] for obj in objects_in_bucket if obj["Key"].endswith(".pdf")]

    # Create a MongoDB client and connect to the collection
    mongo_client = pymongo.MongoClient(MONGO_URI)
    db = mongo_client[MONGODB_NAME]
    collection = db["raw_product_manuals"]

    # Process and insert PDF data, checking for duplicates
    for pdf_path in pdf_files:
        existing_doc = collection.find_one({"path": pdf_path})
        if existing_doc:
            print(f"Skipping duplicate: {pdf_path}")
            continue

        processed_data = list(process_pdf_from_minio(pdf_path))
        pdf_df = spark.createDataFrame(processed_data, ["path", "page_number", "text"])

        pdf_df.write.format("mongo").option("uri", MONGO_URI).option(
            "database", MONGODB_NAME
        ).option("collection", "raw_product_manuals").mode("append").save()
        print(f"Inserted: {pdf_path}")

    spark.stop()
