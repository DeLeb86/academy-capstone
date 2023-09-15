#!/bin/bash
ecruri="338791806049.dkr.ecr.eu-west-1.amazonaws.com/denis"
AWS_REGION="eu-west-1"
dockertag=$1
awstag=$2
docker build -t $dockertag .
token=$(aws ecr get-login-password --region $AWS_REGION)
aws ecr --region $AWS_REGION | docker login -u AWS -p $token $ecruri
docker tag $dockertag $ecruri:$awstag
docker push $ecruri:$awstag