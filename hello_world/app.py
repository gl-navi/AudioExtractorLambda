import json
import os
import time

from utils import check_ffmpeg, check_ffprobe, extract_base_directory, extract_base_name, \
    extract_event_details, get_object_from_s3, save_audio_to_s3, move_original_video_in_s3, \
    get_audio_buffer_from_mp4_bytes, define_keys, get_mongo_db_event_json, fetch_video_manifest_details
from document_db_utils import save_metrics_to_documentdb
from botocore.exceptions import ClientError


def lambda_handler(event, context):
    """
    AWS Lambda function to process an uploaded video file by extracting its audio and reorganizing the S3 bucket.

    This function performs the following tasks:
    1. Extracts the base name of the uploaded video file.
    2. Creates a new directory in the S3 bucket using the base name.
    3. Converts the video file to an audio file (MP3 format).
    4. Saves the extracted audio file to the new directory.
    5. Moves the original video file to the new directory.

    Args:
        event (dict): The event data containing details of the uploaded video file.
        context (LambdaContext): The runtime information of the Lambda function.

    Returns:
        dict: A response object containing the status of the operation.
    """

    start_time = time.time()  # Record the start time

    mongodb_APIgateway_uri = os.getenv("documentDB_url")

    try:

        # Extract bucket name and key from the event
        bucket, key = extract_event_details(event)

        # Extract base name and define new keys for the new directories
        file_base_name = extract_base_name(key=key)
        directory_name = extract_base_directory(key=key)

        video_file_bytes = get_object_from_s3(bucket, key)

        # Extract audio and save it to S3
        audio_buffer = get_audio_buffer_from_mp4_bytes(video_file_bytes, "wav")

        if not audio_buffer:
            raise ValueError(f"Audio buffer could not be created from MP4 bytes for {bucket}.")

        wav_key = f"{directory_name}/audio.wav"

        print(f"directory_name >>> {directory_name} file_base_name >>> {file_base_name}")

        save_audio_to_s3(bucket, wav_key, audio_buffer)

        wav_key, new_video_key = define_keys(file_base_name=file_base_name)

        # Move the original video file
        move_original_video_in_s3(bucket, key, new_video_key)

        # # Fetch parameters from the video manifest
        video_manifest = fetch_video_manifest_details(
            bucket=bucket,
            source_dir=file_base_name)
        #
        result_json = get_mongo_db_event_json(event_name=file_base_name, video_manifest=video_manifest)
        #
        save_metrics_to_documentdb(mongodb_APIgateway_uri=mongodb_APIgateway_uri, db_name="gl-document-db",
                                   collection_name="gl-pipeline-events",
                                   event_json=result_json)

        elapsed_time = time.time() - start_time
        elapsed_minutes = elapsed_time / 60
        print(f"Total time taken: {elapsed_minutes:.2f} minutes")

        return {
            "statusCode": 200,
            "body": json.dumps({
                "message": f"Audio file successfully extracted and saved to {wav_key} in {bucket}.",
            }),
        }


    except KeyError as e:
        return {
            "statusCode": 400,
            "body": json.dumps({
                "message": "Error processing event. Key not found.",
            }),
        }

    except ClientError as e:
        return {
            "statusCode": 500,
            "body": json.dumps({
                "message": "Error interacting with S3.",
            }),
        }

    except Exception as e:
        return {
            "statusCode": 500,
            "body": json.dumps({
                "message": "Internal server error.",
            }),
        }
