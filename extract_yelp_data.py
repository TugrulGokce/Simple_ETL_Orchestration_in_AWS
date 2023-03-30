import json
import boto3


def upload_file_to_s3(local_file_path, remote_file_path, s3_bucket_name='awsbc4hello'):
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
    with open(local_file_path, "rb") as f:
        s3_client.upload_fileobj(f, s3_bucket_name, remote_file_path)


def extract_yelp_data_and_upload_s3_bucket(file_name):
    with open(file_name, 'r', encoding="utf-8") as f:
        for index, line in enumerate(f):
            data = json.loads(line)
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
            user_id = data['user_id']
            print(f"{user_data=}")
            # TODO dosya ismi olarak uuid kullanmak mantıklı gibi
            # upload_file_to_s3(data, f"{file_name.split('_')[-1]}/{data}")
            # print(data)
            if index == 100:
                break


if __name__ == '__main__':
    # extract_data_from_yelp(r"D:\Zip\yelp_academic_dataset_review.json")
    # user icin friends kısmı uzun geliyor
    extract_yelp_data_and_upload_s3_bucket(r"D:\Zip\yelp_academic_dataset_user.json")

"""
import uuid

# Generate a UUID string
uuid_str = str(uuid.uuid4())

# Print the UUID string
print(uuid_str)

"""
