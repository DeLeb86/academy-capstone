from load_data import read_data,transform_data,read_from_json,init_spark_session
from load_to_sf import load_to_snowflake
import os
s3=os.getenv("S3")
frame=read_data(s3,init_spark_session("ingestor"))
#frame=read_from_json("resources/data_upload",init_spark_session(ingestor))
print("dataloaded..")
frame=transform_data(frame)
print("data transformed..")
### load to snowflake ###
load_to_snowflake(frame)
print("data uploaded..")