#!/bin/bash

# Get AWS account ID
AWS_ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
AWS_REGION="us-east-1"
IMAGE_NAME="prismix-blur-detection"
STAGE=${1:-dev}  # Use first argument as stage, default to dev

# Create ECR repository if it doesn't exist
# if ! aws ecr describe-repositories --repository-names ${IMAGE_NAME} --region $AWS_REGION 2>/dev/null; then
#     echo "Creating ECR repository ${IMAGE_NAME}..."
#     aws ecr create-repository --repository-name ${IMAGE_NAME} --region $AWS_REGION
# fi

# ECR login
aws ecr get-login-password --region $AWS_REGION | docker login --username AWS --password-stdin $AWS_ACCOUNT_ID.dkr.ecr.$AWS_REGION.amazonaws.com

# Build the image
docker build -t $IMAGE_NAME:$STAGE ./src/blur-detection

# Tag the image
docker tag $IMAGE_NAME:$STAGE $AWS_ACCOUNT_ID.dkr.ecr.$AWS_REGION.amazonaws.com/$IMAGE_NAME:$STAGE

# Push the image
docker push $AWS_ACCOUNT_ID.dkr.ecr.$AWS_REGION.amazonaws.com/$IMAGE_NAME:$STAGE 