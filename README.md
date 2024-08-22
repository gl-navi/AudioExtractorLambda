# AudioExtractorLambda

AWS Lambda  to process an uploaded video file by extracting its audio and reorganizing the S3 bucket.

    This function performs the following tasks:
    1. Extracts the base name of the uploaded video file.
    2. Creates a new directory in the S3 bucket using the base name.
    3. Converts the video file to an audio file (MP3 format).
    4. Saves the extracted audio file to the new directory.
    5. Moves the original video file to the new directory.