import json
import cv2
import numpy as np
import base64
import logging
import re
from typing import Dict, Union, Any

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

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
        - blur_score: Combined score (0-500 range, lower means more blurry)
        - is_blurry: Boolean based on threshold (100)
    """
    # Convert to grayscale
    gray = cv2.cvtColor(img, cv2.COLOR_BGR2GRAY)
    
    # 1. Laplacian variance (primary metric)
    laplacian = cv2.Laplacian(gray, cv2.CV_64F)
    laplacian_score = laplacian.var()
    
    # 2. Sobel derivatives
    sobel_x = cv2.Sobel(gray, cv2.CV_64F, 1, 0, ksize=3)
    sobel_y = cv2.Sobel(gray, cv2.CV_64F, 0, 1, ksize=3)
    sobel_score = (sobel_x.var() + sobel_y.var()) / 2
    
    # 3. FFT-based score
    rows, cols = gray.shape
    crow, ccol = rows//2, cols//2
    fft = np.fft.fft2(gray)
    fft_shift = np.fft.fftshift(fft)
    fft_score = np.abs(fft_shift[crow-30:crow+30, ccol-30:ccol+30]).var()
    
    # Normalize scores to 0-500 range
    normalized_score = float(
        (0.60 * min(laplacian_score / 100, 500) +
         0.25 * min(sobel_score / 100, 500) +
         0.15 * min(fft_score / 10000, 500)
        )
    )
    
    # Ensure the final score is capped at 500
    final_score = min(normalized_score, 500)
    
    metrics = {
        'combinedScore': final_score,
        'isBlurry': bool(final_score < 100)
    }
    
    logger.debug(f"Calculated blur metrics: {metrics}")
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

def handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """
    Lambda handler for blur detection.
    
    Args:
        event: Lambda event
        context: Lambda context
        
    Returns:
        API Gateway response
    """
    try:
        # Parse event body
        try:
            body = event['body'] if isinstance(event['body'], dict) else json.loads(event['body'])
        except (KeyError, json.JSONDecodeError) as e:
            logger.error(f"Failed to parse event body: {str(e)}")
            return create_response(400, {
                'error': 'Invalid request body',
                'details': str(e)
            })

        # Validate input
        if 'imageBuffer' not in body:
            logger.error("Missing imageBuffer in request body")
            return create_response(400, {
                'error': 'Missing imageBuffer in request body'
            })

        # Process image
        try:
            # Decode image
            img = decode_image(body['imageBuffer'])
            
            # Calculate blur metrics
            blur_metrics = calculate_blur_metrics(img)
            
            logger.info(f"Successfully processed image with blur metrics: {blur_metrics}")
            return create_response(200, blur_metrics)
            
        except ValueError as e:
            logger.error(f"Image processing error: {str(e)}")
            return create_response(400, {
                'error': 'Image processing failed',
                'details': str(e)
            })
            
    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}", exc_info=True)
        return create_response(500, {
            'error': 'Internal server error',
            'details': str(e)
        }) 
    
# test_event = {
#     "body": {
#         "imageBuffer": "/9j/4AAQSkZJRgABAQEASABIAAD/..." # base64 image
#     }
# }

# # Local test
# result = handler(test_event, None)
# print(json.dumps(result, indent=2))   