import os
import subprocess
from typing import Tuple, Any

import boto3
import re
import io
from pydub import AudioSegment
import urllib.parse
import json
from datetime import datetime
from zoneinfo import ZoneInfo

s3Client = boto3.client("s3")


def check_ffmpeg():
    """
        Check for the presence and functionality of the `ffmpeg` command-line tool.

        This function performs the following steps:
        1. Lists the contents of the `/opt/bin` directory to verify the presence of `ffmpeg`.
        2. Attempts to execute the `ffmpeg` command without arguments to ensure it is available.
        3. Runs `ffmpeg` with the `-version` flag to retrieve and print version information.
        4. Checks the exit code of the `ffmpeg` command to confirm successful execution.

        Outputs:
            - Directory listing of `/opt/bin` showing `ffmpeg`.
            - Result of the `ffmpeg` command execution.
            - `ffmpeg` version information if available.
            - Error messages if `ffmpeg` is not found or fails to execute.
    """
    # Print the contents of the /opt/bin directory to check for the presence of ffmpeg
    print("ffmpeg File in lib >>>")
    print(os.system('ls -la /opt/bin'))

    # Execute the ffmpeg command without arguments and print the result
    print(os.system('ffmpeg'))
    print("should be above ffmpeg File>>>")

    # Run ffmpeg with the -version flag to get version information
    result = subprocess.run(['/opt/bin/ffmpeg', '-version'], capture_output=True, text=True)

    # Print the version information of ffmpeg
    print(f"ffmpeg version info: ${result}")
    print(result.stdout)

    # Check if the ffmpeg command executed successfully
    if result.returncode != 0:
        print("ffmpeg command failed with return code:", result.returncode)
        print("stderr:", result.stderr)
    else:
        print("ffmpeg is available and working.")


def check_ffprobe():
    """
        Check for the presence and functionality of the `ffprobe` command-line tool.

        This function performs the following steps:
        1. Lists the contents of the `/opt/bin` directory to verify the presence of `ffprobe`.
        2. Attempts to execute the `ffprobe` command without arguments to ensure it is available.
        3. Runs `ffprobe` with the `-version` flag to retrieve and print version information.
        4. Checks the exit code of the `ffprobe` command to confirm successful execution.

        Outputs:
            - Directory listing of `/opt/bin` showing `ffprobe`.
            - Result of the `ffprobe` command execution.
            - `ffprobe` version information if available.
            - Error messages if `ffprobe` is not found or fails to execute.
    """
    # Print the contents of the /opt/bin directory to check for the presence of ffprobe
    print("ffprobe File in /opt/bin >>>")
    print(os.system('ls -la /opt/bin'))

    # Execute the ffprobe command without arguments and print the result
    print(os.system('ffprobe'))
    print("should be above ffprobe File>>>")

    # Run ffprobe with the -version flag to get version information
    result = subprocess.run(['/opt/bin/ffprobe', '-version'], capture_output=True, text=True)

    # Print the version information of ffprobe
    print(f"ffprobe version info: {result.stdout}")

    # Check if the ffprobe command executed successfully
    if result.returncode != 0:
        print("ffprobe command failed with return code:", result.returncode)
        print("stderr:", result.stderr)
    else:
        print("ffprobe is available and working.")


def extract_base_name(key: str) -> str:
    """
    Extract the base name of the file (without the extension) from the S3 object key.

    Args:
        key (str): The S3 object key.

    Returns:
        str: The base name of the file.
    """
    return os.path.splitext(os.path.basename(key))[0]


def extract_base_directory(key: str) -> str:
    """
    Extract the base directory (the part before the first '/') from the S3 object key.

    Args:
        key (str): The S3 object key.

    Returns:
        str: The base directory.
    """
    return key.split('/')[0]


def extract_num_speakers(key: str) -> int:
    """
    Extract the number of speakers (ns) from the S3 object key.

    Args:
        key (str): The S3 object key.

    Returns:
        int: The number of speakers.
    """
    base_name = os.path.splitext(os.path.basename(key))[0]
    match = re.search(r'_ns_(\d+)', base_name)

    if match:
        return int(match.group(1))
    raise ValueError("Number of speakers (ns) not found in the filename.")


def define_keys(file_base_name: str) -> tuple:
    """
    Define the new directory and file keys for the MP3 and video files.

    Args:
        file_base_name (str): The base name of the file.

    Returns:
        tuple: The keys for the MP3 file and the new video file.
    """
    new_directory = f"{file_base_name}/"
    wav_key = f"{file_base_name}/audio.wav"
    new_video_key = f"{new_directory}video.mp4"
    return wav_key, new_video_key


def get_object_from_s3(bucket: str, key: str) -> bytes:
    """
    Get an object from S3 as bytes.

    Args:
        bucket (str): The name of the S3 bucket.
        key (str): The key of the S3 object.

    Returns:
        bytes: The content of the S3 object.
    """

    try:
        response = s3Client.get_object(Bucket=bucket, Key=key)
        return response['Body'].read()
    except Exception as e:
        print(e)
        return None


def save_audio_to_s3(bucket: str, wav_key: str, audio_buffer: io.BytesIO):
    """
    Save the MP3 audio file to S3.

    Args:
        bucket (str): The name of the S3 bucket.
        wav_key (str): The key for the MP3 file.
        audio_buffer (io.BytesIO): The bytes buffer containing the MP3 audio data.
    """
    print(f"Saving audio file to {bucket}/{wav_key}")

    s3Client.put_object(
        Bucket=bucket,
        Key=wav_key,
        Body=audio_buffer,
        ContentType='audio/wav'
    )


def move_original_video_in_s3(bucket: str, key: str, new_video_key: str):
    """
    Move the original video file to a new directory in S3.

    Args:
        bucket (str): The name of the S3 bucket.
        key (str): The current key of the video file.
        new_video_key (str): The new key for the video file.
    """
    s3Client.copy_object(
        Bucket=bucket,
        CopySource={'Bucket': bucket, 'Key': key},
        Key=new_video_key
    )
    s3Client.delete_object(Bucket=bucket, Key=key)


def extract_event_details(event: dict) -> tuple:
    """
    Extract the bucket name and key from the S3 event, and decode the key.

    Args:
        event (dict): The event data containing details of the uploaded video file.

    Returns:
        tuple: The bucket name and the decoded key of the S3 object.
    """
    bucket = event["Records"][0]["s3"]["bucket"]["name"]
    key = event["Records"][0]["s3"]["object"]["key"]
    decoded_key = urllib.parse.unquote(key)  # Decode the URL-encoded key
    return bucket, decoded_key


def pydub_audiosegment2buffer(segment: AudioSegment, audio_format: str) -> io.BytesIO:
    """
        Convert a pydub AudioSegment to a bytes buffer in a specified audio format.

        Args:
            segment (pydub.AudioSegment): The input AudioSegment to be converted.
            audio_format (str): The target audio format for the export (e.g., "wav", "mp3").

        Returns:
            io.BytesIO: A bytes buffer containing the audio data in the specified format.

        Example:
            buffer = pydub_audiosegment2buffer(audio_segment, "mp3")
            # Now `buffer` contains the MP3-encoded audio data.
    """
    buffer = io.BytesIO()
    segment.export(buffer, format=audio_format)

    return buffer


def get_audio_buffer_from_mp4_bytes(audio_file_bytes: bytes, audio_format: str = "wav") -> io.BytesIO:
    """
    Encode MP4 audio data (in bytes) to a specified audio format and return it as a bytes buffer.

    This function reads MP4-encoded audio data from a bytes object, converts it to the desired
    audio format (default is "wav"), and returns the result as an in-memory bytes buffer.

    Args:
        audio_file_bytes (bytes): The input MP4 audio data in bytes.
        audio_format (str): The target audio format for the conversion (default: "wav").

    Returns:
        io.BytesIO: A bytes buffer containing the audio data in the specified format.

    Example:
        wav_buffer = get_audio_buffer_from_mp4_bytes(mp4_data)
        # Now `wav_buffer` contains the WAV-encoded audio data.
    """

    # Load the MP4 audio data from the bytes object into a pydub AudioSegment
    sound = AudioSegment.from_file(io.BytesIO(audio_file_bytes), "mp4")

    # Convert the AudioSegment to a bytes buffer in the specified audio format
    sound_buffer = pydub_audiosegment2buffer(sound, audio_format)

    # Return the buffer containing the audio data
    return sound_buffer


def get_job_type(video_manifest: dict) -> dict:
    """
    Extracts the jobType from the video manifest content.

    Args:
        video_manifest (dict): The video manifest as a dictionary.

    Returns:
        dict: The "jobType" object from the manifest.
    """
    try:
        # Get the 'jobType' field from the manifest content
        job_type = video_manifest.get("jobType", {})

        return job_type

    except Exception as e:
        raise RuntimeError(f"Unexpected error: {str(e)}")


def create_mongo_db_event_json(event_name, job_type, video_manifest):
    """
        Creates a structured JSON event document for MongoDB based on job type and video manifest data.

        This function dynamically constructs an event JSON object based on whether the job type
        includes "analysis" and/or "voiceExtraction". It extracts relevant details from the video manifest
        and organizes them accordingly.

        Args:
            event_name (str): The name of the event being processed.
            job_type (dict): A dictionary indicating which job types are enabled (e.g., {"analysis": True, "voiceExtraction": False}).
            video_manifest (dict): The video manifest containing metadata such as known participants, speaker count, and email.

        Returns:
            dict: A structured JSON event object containing event details and metadata.
                  Returns None if an error occurs.

        Example:
            Input:
                event_name = "Test_Event"
                job_type = {"analysis": True, "voiceExtraction": True}
                video_manifest = {
                    "jwids": ["JW123", "JW456"],
                    "number_of_speakers": 2,
                    "result_email": "example@gmail.com",
                    "unknown_speaker_jwid": "UNK001",
                    "known_speaker_jwid": "JW123"
                }

            Output:
                {
                    "event_details": {
                        "event_name": "Test_Event",
                        "jobType": ["analysis", "voice_extraction"],
                        "known_participants_jw_ids": ["JW123", "JW456"],
                        "known_number_of_speakers": 2,
                        "result_email": "example@gmail.com",
                        "unknown_speaker_jwid": "UNK001",
                        "known_speaker_jwid": "JW123"
                    },
                    "metadata": {
                        "created_at": "2025-02-17T15:30:45.123+09:00",
                        "updated_at": "2025-02-17T15:30:45.123+09:00"
                    }
                }
        """
    try:
        # Set local timezone (Japan Standard Time)
        local_timezone = ZoneInfo("Asia/Tokyo")
        current_timestamp = datetime.now(local_timezone).isoformat()

        # Initialize event structure
        event_json = {
            "event_details": {
                "event_name": event_name,
                "jobType": [],  # Will dynamically add job types
            },
            "metadata": {
                "created_at": current_timestamp,
                "updated_at": current_timestamp,
            }
        }

        # If 'analysis' is enabled, add analysis-specific data
        if job_type.get("analysis"):
            event_json["event_details"]["jobType"].append("analysis")
            event_json["event_details"]["known_participants_jw_ids"] = video_manifest.get("jwids", [])
            event_json["event_details"]["known_number_of_speakers"] = video_manifest.get("number_of_speakers", None)
            event_json["event_details"]["result_email"] = video_manifest.get("result_email", "")

        # If 'voiceExtraction' is enabled, add voice extraction-specific data
        if job_type.get("voiceExtraction"):
            event_json["event_details"]["jobType"].append("voice_extraction")
            event_json["event_details"]["unknown_speaker_jwid"] = video_manifest.get("unknown_speaker_jwid", "")
            event_json["event_details"]["known_speaker_jwid"] = video_manifest.get("known_speaker_jwid", "")

        return event_json

    except Exception as e:
        print(f"Error occurred while creating MongoDB event JSON: {str(e)}")
        return None


def get_mongo_db_event_json(event_name, video_manifest: dict):
    """
        Generates a MongoDB event JSON by extracting job type from the video manifest.

        This function retrieves the job type from the video manifest and then calls
        `create_mongo_db_event_json` to construct the event JSON.

        Args:
            event_name (str): The name of the event being processed.
            video_manifest (dict): The video manifest containing metadata such as known participants,
                                   speaker count, job type, and email.

        Returns:
            dict: A structured JSON event object containing event details and metadata.

        Error Handling:
            - If an exception occurs, it returns an error response with status code 500
              and an error message.

        Example:
            Input:
                event_name = "Test_Event"
                video_manifest = {
                    "jobType": {"analysis": True, "voiceExtraction": True},
                    "jwids": ["JW123", "JW456"],
                    "number_of_speakers": 2,
                    "result_email": "example@gmail.com",
                    "unknown_speaker_jwid": "UNK001",
                    "known_speaker_jwid": "JW123"
                }

            Output:
                {
                    "event_details": {
                        "event_name": "Test_Event",
                        "jobType": ["analysis", "voice_extraction"],
                        "known_participants_jw_ids": ["JW123", "JW456"],
                        "known_number_of_speakers": 2,
                        "result_email": "example@gmail.com",
                        "unknown_speaker_jwid": "UNK001",
                        "known_speaker_jwid": "JW123"
                    },
                    "metadata": {
                        "created_at": "2025-02-17T15:30:45.123+09:00",
                        "updated_at": "2025-02-17T15:30:45.123+09:00"
                    }
                }
        """
    try:

        # Call the get_job_type function to fetch the job type from the manifest
        job_type = get_job_type(video_manifest)

        event_json = create_mongo_db_event_json(event_name=event_name, job_type=job_type, video_manifest=video_manifest)

        return event_json

    except Exception as e:
        print(f"Error occurred while creating MongoDB event JSON: {str(e)}")
        return {
            "statusCode": 500,
            "body": f"Error occurred: {str(e)}"
        }


def fetch_video_manifest_details(bucket: str, source_dir: str) -> dict:
    """
    Fetch details from the video manifest file, including the number of speakers and optional parms.

    Args:
        bucket (str): S3 bucket name.
        directory_name (str): Directory containing the manifest file.

    Returns:
        tuple: (number_of_speakers, onset_param), where onset_param is None if not present.
        :param bucket:
        :param source_dir:
        :param dir_name:
    """

    try:

        manifest_key = f"{source_dir}/videoManifest.json"

        response = s3Client.get_object(Bucket=bucket, Key=manifest_key)

        manifest_content = json.loads(response["Body"].read().decode('utf-8'))

        return manifest_content

    except s3Client.exceptions.NoSuchKey:
        raise FileNotFoundError(f"Manifest file not found at {manifest_key}")

    except json.JSONDecodeError:
        raise ValueError("Manifest file is not a valid JSON document.")
