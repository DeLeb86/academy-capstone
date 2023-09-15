#!/usr/bin
docker build -t $1 .
docker run -it --env AWS_ACCESS_KEY_ID=$AWS_ACCESS_KEY_ID --env AWS_SECRET_ACCESS_KEY=$AWS_SECRET_ACCESS_KEY -t $1