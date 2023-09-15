import requests
import boto3,os,json

def get_air_data(limit:int=10000,page:int=1) -> list:
    url = f"https://api.openaq.org/v2/measurements?limit={limit}&page={page}&offset=0&sort=desc&parameter=no2&radius=1000&country=BE&order_by=datetime"
    headers = {"accept": "application/json"}
    response = requests.get(url, headers=headers)
    return response.json()["results"]

def select_data(data) : 
    return [k for k in data if k["parameter"] == "no2"]
def store_to_s3(data,s3=None):
    if s3 is None : s3=os.getenv("S3IN")
    s3_client=boto3.client("s3",region_name="eu-west-1")
    s3_client.put_object(
        Body=json.dumps(data),
        Bucket="dataminded-academy-capstone-resources",
        Key="Denis/ingest/air-quality-data.json"
    
    )
    
if __name__ == "__main__" : 
    data=get_air_data()
    
