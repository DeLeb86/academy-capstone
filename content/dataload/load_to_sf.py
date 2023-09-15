import boto3,json
from pyspark.sql import SparkSession,DataFrame,functions as sf
from pyspark import SparkConf
import os

def get_secret() -> dict:
    print(os.getenv("AWS_REGION"))
    sm=boto3.client("secretsmanager",region="eu-west-1")
    secret=sm.get_secret_value(SecretId="arn:aws:secretsmanager:eu-west-1:338791806049:secret:snowflake/capstone/login-uTKlGA")
    return json.loads(secret["SecretString"])

def load_to_snowflake(frame : DataFrame) :
    secret=get_secret()
    frame.write.format("snowflake").options( **{
        "sfurl" : secret["URL"],
        "sfuser" : secret["USER_NAME"],
        "sfpassword" : secret["PASSWORD"],
        "sfdatabase" : secret["DATABASE"],
        "sfschema" : "DENIS",
        "sfwarehouse":secret["WAREHOUSE"],
        "sfrole" : secret["ROLE"],
        "dbtable" : "capstoneData"
        }).mode("overwrite").save()
    

       

if __name__=="__main__":
    print(get_secret())
    