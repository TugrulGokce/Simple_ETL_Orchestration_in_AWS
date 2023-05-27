import boto3
import time
import io


def calculate_passing_time(func):
    """
    It takes a function as an argument, and returns a function that will print the time it takes to run
    the original function

    :param func: The function to be decorated
    :return: The wrapper function.
    """

    def wrapper(*args, **kwargs):
        start_time = time.time()
        result = func(*args, **kwargs)
        end_time = time.time()
        print(f"For {func.__name__!r} passing time {(end_time - start_time):.4f}s")
        return result

    return wrapper


def upload_file_to_s3(data, remote_file_path, s3_bucket_name='yelp-data-bucket1'):
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
