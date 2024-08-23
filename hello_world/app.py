import io
import json
import os
import subprocess
import boto3
from pydub import AudioSegment
from botocore.exceptions import ClientError

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


def define_keys(file_base_name: str) -> tuple:
    """
    Define the new directory and file keys for the MP3 and video files.

    Args:
        file_base_name (str): The base name of the file.

    Returns:
        tuple: The keys for the MP3 file and the new video file.
    """
    new_directory = f"data/{file_base_name}/"
    mp3_key = f"data/{file_base_name}/audio.mp3"
    new_video_key = f"{new_directory}video.mp4"
    return mp3_key, new_video_key


def get_object_from_s3(bucket: str, key: str) -> bytes:
    """
    Get an object from S3 as bytes.

    Args:
        bucket (str): The name of the S3 bucket.
        key (str): The key of the S3 object.

    Returns:
        bytes: The content of the S3 object.
    """
    response = s3Client.get_object(Bucket=bucket, Key=key)
    return response['Body'].read()


def save_audio_to_s3(bucket: str, mp3_key: str, audio_buffer: io.BytesIO):
    """
    Save the MP3 audio file to S3.

    Args:
        bucket (str): The name of the S3 bucket.
        mp3_key (str): The key for the MP3 file.
        audio_buffer (io.BytesIO): The bytes buffer containing the MP3 audio data.
    """
    s3Client.put_object(
        Bucket=bucket,
        Key=mp3_key,
        Body=audio_buffer,
        ContentType='audio/mpeg'
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
    Extract the bucket name and key from the S3 event.

    Args:
        event (dict): The event data containing details of the uploaded video file.

    Returns:
        tuple: The bucket name and the key of the S3 object.
    """
    bucket = event["Records"][0]["s3"]["bucket"]["name"]
    key = event["Records"][0]["s3"]["object"]["key"]
    return bucket, key


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


def get_audio_buffer_from_mp4_bytes(audio_file_bytes: bytes, audio_format: str) -> io.BytesIO:
    """
    Encode MP4 audio data (in bytes) to a specified audio format and return it as a bytes buffer.

    This function reads MP4-encoded audio data from a bytes object, converts it to the desired
    audio format (e.g., MP3), and returns the result as an in-memory bytes buffer.

    Args:
        audio_file_bytes (bytes): The input MP4 audio data in bytes.
        audio_format (str): The target audio format for the conversion (e.g., "mp3").

    Returns:
        io.BytesIO: A bytes buffer containing the audio data in the specified format.

    Example:
        mp3_buffer = get_audio_buffer_from_mp4_bytes(mp4_data, "mp3")
        # Now `mp3_buffer` contains the MP3-encoded audio data.
    """

    # Load the MP4 audio data from the bytes object into a pydub AudioSegment
    sound = AudioSegment.from_file(io.BytesIO(audio_file_bytes), "mp4")

    # Convert the AudioSegment to a bytes buffer in the specified audio format
    sound_buffer = pydub_audiosegment2buffer(sound, audio_format)

    # Return the buffer containing the audio data
    return sound_buffer


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

    try:
        # Extract bucket name and key from the event
        bucket, key = extract_event_details(event)

        # Extract base name and define new keys for the new directories
        file_base_name = extract_base_name(key)

        mp3_key, new_video_key = define_keys(file_base_name)

        # Get the video file from S3
        video_file_bytes = get_object_from_s3(bucket, key)

        # Extract audio and save it to S3
        audio_buffer = get_audio_buffer_from_mp4_bytes(video_file_bytes, "mp3")

        if not audio_buffer:
            raise ValueError("Audio buffer could not be created from MP4 bytes.")

        save_audio_to_s3(bucket, mp3_key, audio_buffer)

        # Move the original video file
        move_original_video_in_s3(bucket, key, new_video_key)

        return {
            "statusCode": 200,
            "body": json.dumps({
                "message": f"Audio file successfully extracted and saved to {mp3_key} in {bucket}.",
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
