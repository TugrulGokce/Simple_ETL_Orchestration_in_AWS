import json
import uuid
from pprint import pprint
from utils import calculate_passing_time, upload_file_to_s3


@calculate_passing_time
def upload_yelp_review_data_to_s3_bucket(review_file_path):
    review_records = []
    print("Uploading data to 'review' folder in S3 Bucket.")
    with open(review_file_path, 'r', encoding="utf-8") as f:
        s3_file_name = review_file_path.split('_')[-1].split('.')[0]
        print(f"uploading {s3_file_name} file to S3")
        for index, line in enumerate(f):
            if index == 1000:
                print("yelp_review data upload process finished here")
                break
            data = json.loads(line)
            review_records.append(data)

    upload_file_to_s3(json.dumps(review_records), "review-yelp.json")


@calculate_passing_time
def upload_yelp_user_data_to_s3_bucket(user_file_path: str, user_id_list: list):
    user_records = []
    print("Uploading data to 'user' folder in S3 Bucket.")
    for user_id in user_id_list:
        with open(user_file_path, 'r', encoding="utf-8") as user_lines:
            for line in user_lines:
                data = json.loads(line)
                if data['user_id'] == user_id:
                    user_records.append(data)
                    break
    upload_file_to_s3(json.dumps(user_records), "user-yelp.json")
    print("'user' upload process finished.")


@calculate_passing_time
def upload_yelp_business_data_to_s3_bucket(business_file_path: str, business_id_list: list):
    business_record = []
    print("Uploading data to 'business' folder in S3 Bucket.")
    for business_id in business_id_list:
        with open(business_file_path, 'r', encoding="utf-8") as business_lines:
            for line in business_lines:
                data = json.loads(line)
                if data['business_id'] == business_id:
                    business_record.append(data)
                    break
    upload_file_to_s3(json.dumps(business_record), "business-yelp.json")
    print("business' upload process finished.")


def find_associated_ids_with_first_thousand_review_data(review_json_path: str):
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


if __name__ == '__main__':
    user_ids, business_ids = find_associated_ids_with_first_thousand_review_data(
        r"D:\Zip\yelp_academic_dataset_review.json")
    upload_yelp_review_data_to_s3_bucket(r"D:\Zip\yelp_academic_dataset_review.json")
    upload_yelp_business_data_to_s3_bucket(r"D:\Zip\yelp_academic_dataset_business.json", business_ids)
    upload_yelp_user_data_to_s3_bucket(r"D:\Zip\yelp_academic_dataset_user.json", user_ids)
