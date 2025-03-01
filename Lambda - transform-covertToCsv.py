import boto3
import json
import pandas as pd 

s3_client = boto3.client('s3')

def lambda_handler(event,context):
    source_bucket = event['Records'][0]['s3']['bucket']['name']
    object_key = event['Records'][0]['s3']['object']['key']
    target_bucket = 'api-aws-pipeline-bucket3'
    #copy the same name without '.json'
    target_file_name = object_key[:-5]   
    print('file name',target_file_name)

    waiter = s3_client.get_waiter('object_exists')
    #make sure the file is loaded completely
    waiter.wait(Bucket=source_bucket, Key=object_key)

    response = s3_client.get_object(Bucket=source_bucket,Key=object_key)
    print('step1',response)
    data=response['Body']
    print('step2',response)
    data = response['Body'].read().decode('utf-8')
    print('step3',response)
    data =json.loads(data)
    print('step4',data)

    f=[]
    #loop all items in the json file
    for i in data['results']:
        f.append(i)
    df=pd.DataFrame(f)
    selected_columns = ['bathrooms','bedrooms','city','homeStatus','homeType','livingArea','price','rentZestimate','zipcode']
    df=df[selected_columns]

    #convert to csv and load to s3
    csv_data=df.to_csv(index=False)
    bucket_name=target_bucket
    object_key=f"{target_file_name}.csv"
    s3_client.put_object(Bucket=bucket_name,Key=object_key,Body=csv_data)

    return {
        'statusCode': 200,
        'body': json.dumps('Copy & Conversion completed successfully!')
    }
