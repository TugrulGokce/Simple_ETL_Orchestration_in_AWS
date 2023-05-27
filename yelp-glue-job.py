import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
import pyspark.sql.types as T
import boto3
import json
import awswrangler as wr
from flair.models import TextClassifier
from flair.data import Sentence

sia = TextClassifier.load("en-sentiment")
print("en-sentiment model loaded.")


def sentiment_Flair(x):
    sentence = Sentence(x)
    sia.predict(sentence)
    score = sentence.labels[0]
    if "POSITIVE" in str(score):
        return "positive"
    elif "NEGATIVE" in str(score):
        return "negative"
    else:
        return "neutral"


def read_json_from_s3_and_convert_dynamic_frame(s3_file_path: str) -> DynamicFrame:
    return glueContext.create_dynamic_frame_from_options(
        connection_type='s3',
        connection_options={
            'paths': [s3_file_path]
        },
        format="json",
        format_options={
            "jsonPath": "$[*]",
            "multiline": True
        })


def save_dynamic_frame_to_S3(dynamic_frame: DynamicFrame, s3_file_path: str):
    glueContext.write_dynamic_frame.from_options(
        frame=dynamic_frame,
        connection_type="s3",
        format="parquet",
        connection_options={
            "path": s3_file_path,
        },
        format_options={
            "useGlueParquetWriter": True,
        },
    )


def process_yelp_business_data():
    # Read review yelp JSON file from S3 and save as a dynamic_frame
    business_dynamic_frame = read_json_from_s3_and_convert_dynamic_frame("s3://yelp-data-bucket1/business-yelp.json")
    # Rename fields
    business_dynamic_frame = business_dynamic_frame.rename_field("`name`", "business_name") \
        .rename_field("`review_count`", "review_count_of_business") \
        .rename_field("`stars`", "stars_of_business")

    # Convert string values that look like JSON to JSON
    business_data_list = business_dynamic_frame.toDF().collect()
    business_dict_list = [row.asDict() for row in business_data_list]

    # Convert string nested values that look like JSON to JSON
    for business_data_dict in business_dict_list:
        if isinstance(business_data_dict["hours"], T.Row):
            business_data_dict["hours"] = business_data_dict["hours"].asDict()
        if isinstance(business_data_dict["attributes"], T.Row):
            business_data_dict["attributes"] = business_data_dict["attributes"].asDict()
        if business_data_dict["attributes"]:
            for inner_attributes_key, inner_attributes_value in business_data_dict["attributes"].items():
                if inner_attributes_key in ['Ambience', 'GoodForMeal', 'Music', 'BusinessParking', 'BestNights'] and \
                        business_data_dict["attributes"][inner_attributes_key] not in ['None', '{}', None]:
                    business_data_dict["attributes"][inner_attributes_key] = eval(inner_attributes_value)

    # Create an S3 client
    s3_client = boto3.client('s3')

    # Write the multiline JSON string to S3
    s3_bucket = 'glue-yelp-output'
    s3_key = 'yelp-business-preprocessed/preprocessed_business_yelp.json'
    s3_client.put_object(Body=json.dumps(business_dict_list).encode('utf-8'), Bucket=s3_bucket, Key=s3_key)

    ## Business icin transformasyon islemleri
    preprocessed_business_dynamic_frame = read_json_from_s3_and_convert_dynamic_frame(
        "s3://glue-yelp-output/yelp-business-preprocessed/preprocessed_business_yelp.json")

    # Flatten preprocessed_business_dynamic_frame
    unnested_pre_business_dyf = preprocessed_business_dynamic_frame.unnest()

    # Drop unnecessary fields
    latest_business_dynamic_frame = unnested_pre_business_dyf.drop_fields(
        ["hours", "attributes", 'attributes.Ambience', 'attributes.Music', 'attributes.GoodForMeal',
         'attributes.BusinessParking'])

    # Save processed business dynamic frame to S3 Bucket as .parquet format
    save_dynamic_frame_to_S3(latest_business_dynamic_frame, "s3://glue-yelp-output/yelp-business-processed")


def process_yelp_user_data():
    # Read user yelp JSON file from S3 and save as a dynamic_frame
    user_dynamic_frame = read_json_from_s3_and_convert_dynamic_frame("s3://yelp-data-bucket1/user-yelp.json")

    # Select fields !!!
    latest_user_dynamic_frame = user_dynamic_frame.select_fields(
        ["user_id", "name", "review_count", "yelping_since", "useful", "cool", "funny", "fans", "average_stars"])

    # rename fields
    latest_user_dynamic_frame = latest_user_dynamic_frame.rename_field("`name`", "user_name").rename_field(
        "`review_count`", "review_count_of_user")

    # Save processed user dynamic frame to S3 Bucket as .parquet format
    save_dynamic_frame_to_S3(latest_user_dynamic_frame, "s3://glue-yelp-output/yelp-user-processed")


def process_yelp_review_data():
    # Read review yelp JSON file from S3 using with awswrangler as a pandas.DataFrame
    review_df = wr.s3.read_json(path='s3://yelp-data-bucket1/review-yelp.json')

    review_df.rename(columns={"stars": "stars_of_review"}, inplace=True)

    # Apply sentiment analysis to review text and create a new column for analysis
    review_df["sentiment_analysis_of_review"] = review_df["text"].apply(sentiment_Flair)

    # Storing data on Data Lake
    wr.s3.to_parquet(
        df=review_df,
        path="s3://glue-yelp-output/yelp-review-processed/processed-yelp-review.parquet"
    )


if __name__ == '__main__':
    ## @params: [JOB_NAME]
    args = getResolvedOptions(sys.argv, ['JOB_NAME'])

    sc = SparkContext()
    glueContext = GlueContext(sc)
    spark = glueContext.spark_session
    job = Job(glueContext)
    job.init(args['JOB_NAME'], args)

    process_yelp_business_data()
    process_yelp_user_data()
    process_yelp_review_data()

    job.commit()
