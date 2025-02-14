import json
import cv2
import numpy as np
import base64
import logging
import re
import hashlib
import os
import boto3
from botocore.exceptions import ClientError
from boto3.dynamodb.conditions import Key
import asyncio
from datetime import datetime
import time
from typing import Dict, Union, Any, Optional

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

def create_hash(value: str) -> str:
    return hashlib.md5(value.encode()).hexdigest()

def clean_base64_string(base64_string: str) -> str:
    """
    Clean and validate base64 image string.
    
    Args:
        base64_string: Raw base64 string, possibly with data URI scheme
        
    Returns:
        str: Cleaned base64 string
        
    Raises:
        ValueError: If string is not a valid base64 image format
    """
    # Remove whitespace
    base64_string = base64_string.strip()
    
    # Check for data URI scheme
    data_uri_pattern = r'^data:image/([a-zA-Z]+);base64,'
    match = re.match(data_uri_pattern, base64_string)
    
    if match:
        # Extract image format and validate
        image_format = match.group(1).lower()
        if image_format not in ['jpeg', 'jpg', 'png']:
            raise ValueError(f"Unsupported image format: {image_format}")
            
        # Remove data URI prefix
        base64_string = re.sub(data_uri_pattern, '', base64_string)
        logger.debug(f"Detected and removed data URI scheme for {image_format} image")
    
    # Remove any remaining whitespace or newlines
    base64_string = ''.join(base64_string.split())
    
    # Validate base64 characters
    if not re.match(r'^[A-Za-z0-9+/]*={0,2}$', base64_string):
        raise ValueError("Invalid base64 characters detected")
    
    return base64_string

def decode_image(base64_string: str) -> np.ndarray:
    """
    Decode base64 string to OpenCV image.
    
    Args:
        base64_string: Base64 encoded image string
        
    Returns:
        np.ndarray: Decoded image
        
    Raises:
        ValueError: If image decoding fails
    """
    try:
        # Clean and validate base64 string
        cleaned_base64 = clean_base64_string(base64_string)
        
        # Decode base64 image
        image_buffer = base64.b64decode(cleaned_base64)
        logger.debug(f"Decoded buffer length: {len(image_buffer)}")
        
        # Verify image header
        if not (image_buffer.startswith(b'\xff\xd8') or  # JPEG
                image_buffer.startswith(b'\x89PNG')):    # PNG
            raise ValueError("Invalid image format: not JPEG or PNG")
        
        # Convert buffer to numpy array and decode
        nparr = np.frombuffer(image_buffer, np.uint8)
        img = cv2.imdecode(nparr, cv2.IMREAD_COLOR)
        
        if img is None:
            raise ValueError("Failed to decode image data")
            
        logger.debug(f"Successfully decoded image with shape: {img.shape}")
        return img
        
    except base64.binascii.Error as e:
        raise ValueError(f"Invalid base64 encoding: {str(e)}")

def calculate_blur_metrics(img: np.ndarray) -> Dict[str, Any]:
    """
    Calculate combined blur score using multiple metrics.
    
    Args:
        img: Input image as numpy array
        
    Returns:
        Dict with combined blur score and blur status:
        - blur_score: Combined score (0-500 range, higher means sharper)
        - is_blurry: Boolean based on threshold (100)
    """
    # Convert to grayscale
    gray = cv2.cvtColor(img, cv2.COLOR_BGR2GRAY)
    
    # Get image size for normalization
    height, width = gray.shape
    pixel_count = height * width
    size_factor = max(1.0, np.sqrt(pixel_count) / 1000)  # Minimum size factor of 1.0
    
    # 1. Laplacian variance (primary metric)
    laplacian = cv2.Laplacian(gray, cv2.CV_64F)
    laplacian_score = laplacian.var() * size_factor
    
    # 2. Sobel derivatives
    sobel_x = cv2.Sobel(gray, cv2.CV_64F, 1, 0, ksize=3)
    sobel_y = cv2.Sobel(gray, cv2.CV_64F, 0, 1, ksize=3)
    sobel_score = (sobel_x.var() + sobel_y.var()) / 2 * size_factor
    
    # 3. FFT-based score
    rows, cols = gray.shape
    crow, ccol = rows//2, cols//2
    fft = np.fft.fft2(gray)
    fft_shift = np.fft.fftshift(fft)
    window_size = max(min(30, min(rows, cols) // 4), 5)
    fft_score = np.abs(fft_shift[
        crow-window_size:crow+window_size, 
        ccol-window_size:ccol+window_size
    ]).var()
    
    # Normalize scores with adjusted scaling
    norm_laplacian = min(laplacian_score * 2, 500)      # Reduced multiplier
    norm_sobel = min(sobel_score / 50, 500)            # Less aggressive division
    norm_fft = min(np.log10(fft_score + 1) * 10, 500)  # Reduced multiplier
    
    # Calculate final score
    final_score = float(
        0.70 * norm_laplacian +  # Primary weight on Laplacian
        0.20 * norm_sobel +      # Secondary weight on Sobel
        0.10 * norm_fft          # Small weight on FFT
    )
    
    # Lower the threshold for blur detection
    final_score = max(0, min(final_score, 500))
    
    metrics = {
        'combinedScore': final_score,
        'isBlurry': bool(final_score < 100),  # Lowered threshold
        'debug': {
            'rawLaplacian': laplacian.var(),
            'laplacianScore': laplacian_score,
            'normalizedLaplacian': norm_laplacian,
            'rawSobel': (sobel_x.var() + sobel_y.var()) / 2,
            'sobelScore': sobel_score,
            'normalizedSobel': norm_sobel,
            'fftScore': fft_score,
            'normalizedFft': norm_fft,
            'imageSize': gray.shape,
            'sizeFactor': size_factor
        }
    }
    
    return metrics

def create_response(status_code: int, body: Dict[str, Any]) -> Dict[str, Any]:
    """Create standardized API response."""
    return {
        'statusCode': status_code,
        'headers': {
            'Content-Type': 'application/json',
            'Access-Control-Allow-Origin': '*'  # CORS support
        },
        'body': json.dumps(body)
    }

def extract_image_id(path_string):
    # Split the string by '/' and get the last element
    image_id = path_string.split('/')[-1]
    return image_id

async def insert_to_task_table(job_id, task_id, evaluation, key):
    # Initialize DynamoDB client
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table(os.environ['TASKS_TABLE'])
    
    # Get current timestamp in milliseconds
    current_time = str(int(time.time() * 1000))
    
    try:
        # First, try to get the existing item
        response = table.get_item(
            Key={
                'JobID': job_id,
                'TaskID': task_id
            }
        )
        
        if 'Item' in response:
            print('Alert: item already processed by worker function...')
            # Item exists, update the evaluation field
            response = table.update_item(
                Key={
                    'JobID': job_id,
                    'TaskID': task_id
                },
                UpdateExpression='SET Evaluation = :eval, UpdatedAt = :time',
                ExpressionAttributeValues={
                    ':eval': evaluation,
                    ':time': current_time
                },
                ReturnValues='ALL_NEW'
            )
            return response['Attributes']
            
        else:
            print('Item has not been processed by the worker function yet...')
            # Item doesn't exist, create new item
            new_item = {
                'JobID': job_id,
                'TaskID': task_id,
                'Evaluation': evaluation,
                'TaskStatus': 'COMPLETED' if evaluation == 'INELIGIBLE' else 'PENDING',
                'Reason': 'Resolution below fhd standard',
                'ImageS3Key': key,
                'UpdatedAt': current_time
            }
            
            # Put the new item in the table
            # Or if the item has not been marked as EXCLUDED yet
            response = table.put_item(
                Item=new_item,
                ConditionExpression='attribute_not_exists(Evaluation) OR #evaluation <> :excluded',
                ExpressionAttributeNames={
                    '#evaluation': 'Evaluation'
                },
                ExpressionAttributeValues={
                    ':excluded': 'EXCLUDED'
                }
            )

            print('Item updated to Task table successfully')
            return new_item
            
    except Exception as e:
        print(f"Error inserting/updating task: {str(e)}")
        raise e
    
async def getProjectSetting(job_id: str) -> Optional[Dict[str, Any]]:
    """
    Retrieve project settings from DynamoDB.
    
    Args:
        job_id: Unique identifier for the job progress table
        
    Returns:
        Dictionary containing project settings or None if not found
        
    Raises:
        ClientError: If there's an error accessing DynamoDB
    """
    try:
        # Initialize DynamoDB client
        dynamodb = boto3.resource('dynamodb')
        table = dynamodb.Table(os.environ['JOB_PROGRESS_TABLE'])
        
        # Get item from DynamoDB
        response = await table.get_item(
            Key={
                'id': job_id
            }
        )
        
        # Check if item exists
        if 'Item' not in response:
            logger.warning(f"Job not found: {job_id}")
            return None
            
        project_setting = response['Item']['projectSetting']
        logger.info(f"Retrieved project setting: {job_id}")
        
        return project_setting
        
    except ClientError as e:
        logger.error(f"Failed to retrieve project setting {job_id}: {str(e)}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error retrieving project setting {job_id}: {str(e)}")
        raise

async def get_file_buffer(bucket: str, key: str) -> bytes:
    """
    Retrieve a file from S3 and convert it to a buffer.
    
    Args:
        bucket: S3 bucket name
        key: Object key in the bucket
        
    Returns:
        File contents as bytes
        
    Raises:
        AppError: If there's an error retrieving the file
    """
    try:
        logger.info('Getting file buffer from S3', extra={'bucket': bucket, 'key': key})
        
        # Initialize S3 client
        s3_client = boto3.client('s3')
        
        # Get file from S3
        response = await s3_client.get_object(
            Bucket=bucket,
            Key=key
        )
        
        # Read the file content
        file_content = await response['Body'].read()
        
        return file_content
        
    except (ClientError, Exception) as error:
        logger.error('Error getting file buffer from S3', 
                    extra={'error': str(error), 'bucket': bucket, 'key': key})
        raise ClientError

def handler (event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    return asyncio.run(handler(event, context))

async def main(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """
    Lambda handler for blur detection triggered by SQS.
    
    Args:
        event: SQS event containing SNS messages
        context: Lambda context
        
    Returns:
        Dictionary with processing results and status
    """
    try:
        # Process each record in the SQS batch
        processing_results = []
        for record in event.get('Records', []):
            try:
                # Parse SNS message from SQS body
                sqs_body = json.loads(record['body'])
                sns_message = json.loads(sqs_body['Message'])
                
                logger.info(f"Processing message: {sns_message}")
                
                # Extract image information
                bucket = sns_message['bucket']
                key = sns_message['key']
                user_id = sns_message['userId']
                project_id = sns_message['projectId']
                job_id = sns_message['jobId']
                project_setting_id = sns_message['projectSettingId']

                # Create a hash based on imageid for inserting new record to Task table
                image_id = extract_image_id(key)
                print(image_id)

                task_id = create_hash(image_id)
                
                try:
                    # Download image from S3
                    img = download_from_s3(bucket, key)

                    # Fetch projectSetting
                    project_setting = await getProjectSetting(job_id)
                    print(project_setting)

                    blurThreshold = project_setting['blurThreshold']
                    print(blurThreshold)
                    
                    # Calculate blur metrics
                    blur_metrics = calculate_blur_metrics(img)
                    print(blur_metrics)

                    # Check if the blur_metrics is less than blurThreshold
                    is_likely_to_be_blurry = blur_metrics < blurThreshold
                    print(is_likely_to_be_blurry)

                    evaluation = 'EXCLUDED' if is_likely_to_be_blurry else 'ELIGIBLE'
                    print(evaluation)

                    # If the image is likely to be blurry, insert a record to the Task table
                    await insert_to_task_table(job_id, task_id, evaluation, key)
                    
                    # Add context information to results
                    result = {
                        'status': 'success',
                        'userId': user_id,
                        'projectId': project_id,
                        'jobId': job_id,
                        'projectSettingId': project_setting_id,
                        'imagePath': key,
                        'blurMetrics': blur_metrics
                    }
                    
                    logger.info(f"Successfully processed image {key} with blur metrics: {blur_metrics}")
                    
                except Exception as e:
                    logger.error(f"Failed to process image {key}: {str(e)}")
                    result = {
                        'status': 'error',
                        'userId': user_id,
                        'projectId': project_id,
                        'jobId': job_id,
                        'projectSettingId': project_setting_id,
                        'imagePath': key,
                        'error': str(e)
                    }
                
                processing_results.append(result)
                
            except json.JSONDecodeError as e:
                logger.error(f"Failed to parse message: {str(e)}")
                processing_results.append({
                    'status': 'error',
                    'error': f'Failed to parse message: {str(e)}'
                })
                
            except KeyError as e:
                logger.error(f"Missing required field in message: {str(e)}")
                processing_results.append({
                    'status': 'error',
                    'error': f'Missing required field: {str(e)}'
                })
        
        return {
            'statusCode': 200,
            'body': {
                'results': processing_results
            }
        }
            
    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}", exc_info=True)
        return {
            'statusCode': 500,
            'body': {
                'error': 'Internal server error',
                'details': str(e)
            }
        }

def download_from_s3(bucket: str, key: str) -> bytes:
    """
    Download image from S3 bucket.
    
    Args:
        bucket: S3 bucket name
        key: Object key
        
    Returns:
        Image bytes
    """
    print('start downliading the image buffer from S3...')

    buffer = get_file_buffer(bucket, key)

    print('buffer retrieved')

    return buffer

    
# test_event = {
#     "body": {
#         "imageBuffer": "" # base64 image
#     }
# }

# # Local test
# result = handler(test_event, None)
# print(json.dumps(result, indent=2))   