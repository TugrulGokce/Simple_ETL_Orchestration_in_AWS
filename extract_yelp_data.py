import json
import boto3
import uuid
from pprint import pprint
import io


def upload_file_to_s3(data, remote_file_path, s3_bucket_name='etlorchestrationbucket'):
    """
    Uploads a local file to an S3 bucket.

    Args:
    local_file_path (str): The file path of the local file to upload.
    s3_bucket_name (str): The name of the S3 bucket to upload to.
    s3_object_key (str): The object key to use for the uploaded file in the S3 bucket.

    Returns:
    None
    """
    s3_client = boto3.client('s3')
    with io.BytesIO(data.encode('utf-8')) as f:
        s3_client.upload_fileobj(f, s3_bucket_name, remote_file_path)


def extract_yelp_data_and_upload_s3_bucket(file_name):
    with open(file_name, 'r', encoding="utf-8") as f:
        s3_file_name = file_name.split('_')[-1].split('.')[0]
        print(f"uploading {s3_file_name} file to S3")
        for index, line in enumerate(f):
            data = json.loads(line)
            if index == 251:
                break
            if s3_file_name == 'user':
                user_data = {
                    "user_id": data["user_id"],
                    "name": data["name"],
                    "review_count": data["review_count"],
                    "yelping_since": data["yelping_since"],
                    "useful": data["useful"],
                    "cool": data["cool"],
                    "funny": data["funny"],
                    "fans": data["fans"],
                    "average_stars": data["average_stars"],
                }
                pprint(user_data)
                upload_file_to_s3(json.dumps(user_data), f"{s3_file_name}/{s3_file_name}-{str(uuid.uuid4())}.json")
            else:
                pprint(data)
            upload_file_to_s3(json.dumps(data), f"{s3_file_name}/{s3_file_name}-{str(uuid.uuid4())}.json")
    print("S3 upload process finished.")


if __name__ == '__main__':
    extract_yelp_data_and_upload_s3_bucket(r"D:\Zip\yelp_academic_dataset_review.json")  # --> 6990280
    extract_yelp_data_and_upload_s3_bucket(r"D:\Zip\yelp_academic_dataset_business.json")  # --> 150346
    extract_yelp_data_and_upload_s3_bucket(r"D:\Zip\yelp_academic_dataset_user.json")  # --> 1987897
