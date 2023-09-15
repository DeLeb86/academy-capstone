FROM public.ecr.aws/dataminded/spark-k8s-glue:v3.1.2-hadoop-3.3.1
WORKDIR /app
ENV S3='s3a://dataminded-academy-capstone-resources/raw/open_aq/'
USER root
COPY requirements.txt /app/
RUN pip3 install --user -r requirements.txt
COPY . /app/ 
ENTRYPOINT [ "python3" ]
CMD [ "main.py" ]