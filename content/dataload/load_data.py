from pyspark import SparkConf
from typing import Collection, Mapping, Union
from pyspark.sql import Column, DataFrame, SparkSession
import pyspark.sql.functions as sf
from pyspark.sql.types import (
    BooleanType,
    ByteType,
    DateType,
    IntegerType,
    ShortType,
    StringType,
    DoubleType
)
ColumnOrStr = Union[Column, str]

def init_spark_session(name:str) -> SparkSession:
    conf=SparkConf()
    conf.set('spark.jars.packages', 'org.apache.hadoop:hadoop-aws:3.1.2,net.snowflake:spark-snowflake_2.12:2.9.0-spark_3.1,net.snowflake:snowflake-jdbc:3.13.3')
    conf.set('spark.hadoop.fs.s3a.aws.credentials.provider', 'com.amazonaws.auth.DefaultAWSCredentialsProviderChain')
    spark=SparkSession.builder.config(conf=conf).appName(name).getOrCreate()
    return spark

def read_data(s3 : str, spark : SparkSession) -> DataFrame: 
    df=spark.read.json(s3)
    return df

def transform_date_types(colname):
    col=sf.regexp_replace(colname,"[+-][0-9]{2}:[0-9]{2}$","")
    col=sf.regexp_replace(col,"T"," ")
    return sf.to_timestamp(col,"yyyy-MM-dd HH:mm:ss")
    

def data_types_correction(frame : DataFrame) -> DataFrame:
    mapping={
        DoubleType : {"value","latitude","longitude"},
        IntegerType: { "locationId"}            
    }
    for datatype, colnames in mapping.items():
        for colname in colnames:
            frame = frame.withColumn(
                colname, sf.col(colname).cast(datatype())
            )
    return frame

def transform_data(frame : DataFrame) -> DataFrame:
    maps={"coordinates":["latitude","longitude"],"date":[("local","local_date"),("utc","utc_date")]}
    for k,l in maps.items():
        for v in l :
            if isinstance(v,str):
                frame=frame.withColumn(v,sf.col(k)[v])
            else:
                frame=frame.withColumn(v[1],sf.col(k)[v[0]])
    frame=data_types_correction(frame)
    for name in ["local_date","utc_date"]:
        frame=frame.withColumn(name,transform_date_types(name))
    frame=frame.drop("date","coordinates","city")
    return frame



def read_from_json(path : str,spark: SparkSession) -> DataFrame:
    frame= spark.read.json(path)   
    return frame

