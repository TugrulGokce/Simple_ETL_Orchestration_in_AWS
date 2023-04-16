import json
import uuid
from pprint import pprint
from utils import calculate_passing_time, upload_file_to_s3


@calculate_passing_time
def upload_yelp_review_data_to_s3_bucket(review_file_path):
    print("Uploading data to 'review' folder in S3 Bucket.")
    with open(review_file_path, 'r', encoding="utf-8") as f:
        s3_file_name = review_file_path.split('_')[-1].split('.')[0]
        print(f"uploading {s3_file_name} file to S3")
        for index, line in enumerate(f):
            if index == 1000:
                print("yelp_review data upload process finished here")
                break
            data = json.loads(line)
            print(f"index = {index + 1} | {data['review_id']}")
            upload_file_to_s3(json.dumps(data), f"review/review-{str(uuid.uuid4())}.json")


def extract_user_data(data):
    return {
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


@calculate_passing_time
def upload_yelp_user_data_to_s3_bucket(user_file_path: str, user_id_list: list):
    count = 0
    print("Uploading data to 'user' folder in S3 Bucket.")
    for idx, user_id in enumerate(user_id_list):
        with open(user_file_path, 'r', encoding="utf-8") as user_lines:
            for index, line in enumerate(user_lines):
                data = json.loads(line)
                if data['user_id'] == user_id:
                    count += 1
                    print(f"{idx=} | {data['user_id']} and {user_id}(from dict) matches in {index + 1} index.")
                    user_data = extract_user_data(data)
                    upload_file_to_s3(json.dumps(user_data), f"user/user-{str(uuid.uuid4())}.json")
                    break
    print(f"uploading count = {count}")
    print("'user' upload process finished.")


@calculate_passing_time
def upload_yelp_business_data_to_s3_bucket(business_file_path: str, business_id_list: list):
    count = 0
    print("Uploading data to 'business' folder in S3 Bucket.")
    for idx, business_id in enumerate(business_id_list):
        with open(business_file_path, 'r', encoding="utf-8") as business_lines:
            for index, line in enumerate(business_lines):
                data = json.loads(line)
                if data['business_id'] == business_id:
                    count += 1
                    print(f"{idx=} | {data['business_id']} and {business_id}(from dict) matches in {index + 1} index.")
                    upload_file_to_s3(json.dumps(data), f"business/business-{str(uuid.uuid4())}.json")
                    break
    print(f"uploading count = {count}")
    print("business' upload process finished.")


def find_associated_ids_with_first_thousand_review_data(review_json_path: str) -> tuple:
    associated_user_ids = []
    associated_business_ids = []

    with open(review_json_path, 'r', encoding="utf-8") as review_file:
        for index, line in enumerate(review_file):
            if index == 1000:
                break
            data = json.loads(line)
            associated_user_ids.append(data['user_id'])
            associated_business_ids.append(data['business_id'])
        # return unique ids
    return [*set(associated_user_ids)], [*set(associated_business_ids)]

# # function that takes s3 bucket name and return s3 bucket object length in 'review' folder.
# def get_s3_bucket_review_data_length(bucket_name: str) -> int:
#     s3_bucket_object = s3.Bucket(bucket_name)
#     review_folder_object = s3_bucket_object.Object('review')
#     return review_folder_object.content_length


if __name__ == '__main__':
    user_ids, business_ids = find_associated_ids_with_first_thousand_review_data(
        r"D:\Zip\yelp_academic_dataset_review.json")
    # upload_yelp_review_data_to_s3_bucket(r"D:\Zip\yelp_academic_dataset_review.json")
    # upload_yelp_business_data_to_s3_bucket(r"D:\Zip\yelp_academic_dataset_business.json", business_ids)
    upload_yelp_user_data_to_s3_bucket(r"D:\Zip\yelp_academic_dataset_user.json", user_ids)
