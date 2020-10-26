import json
import boto3
import os

# boto3 S3 initialization
s3_client = boto3.client("s3")


def lambda_handler(event, context):
    
   # Read Destination Bucket Location from the Environment Object
   destination_bucket_name = os.environ['DESTINATION_BUCKET']
  

   # event contains all information about uploaded object
   print("Event :", event)

   # defining constants for CBCs
   CBC01='cbc01'
   CBC02='cbc02'
   CBC03='cbc03'
   CBC04='cbc04'
   
  
   # Bucket Name where file was uploaded
   source_bucket_name = event['Records'][0]['s3']['bucket']['name']

   # determining which cbc bucket the file came from
   prefix='' 
   
   
   # Filename of object (with path) and Etag
   file_key_name = event['Records'][0]['s3']['object']['key']
   source_etag = event['Records'][0]['s3']['object']['eTag']
   # Filename of object (with path)
   file_key_name = event['Records'][0]['s3']['object']['key']
   print("File name =", file_key_name)

   

   # set prefix based on which bucket it came from
   if CBC01 in source_bucket_name:
       prefix=CBC01
   elif  CBC02 in source_bucket_name:
       prefix=CBC02
   elif  CBC03 in source_bucket_name:
       prefix=CBC03
   elif  CBC04 in source_bucket_name:
       prefix=CBC04        
   else:
       prefix='UNMATCHED'

   # Copy Source Object
   if(prefix != 'UNMATCHED'):
       copy_source_object = {'Bucket': source_bucket_name, 'Key': file_key_name}
       # S3 copy object operation with the desired prefix
       key =prefix+'/'+file_key_name
       s3_client.copy_object(CopySource=copy_source_object, Bucket=destination_bucket_name, Key=key)
       # Read the Etag back after copying the file
       dest_etag = s3_client.head_object(Bucket=destination_bucket_name,Key=key)['ETag'][1:-1]

       if(dest_etag==source_etag):
           print('Copied file correctly')
           # Delete the original file
           s3_client.delete_object(Bucket=source_bucket_name, Key=file_key_name)
           
       statusCode=200
       message='File Processed'
   else:
        statusCode=400
        message='Desired CBC prefix not found'

    # Return appropriate status and response
   return {
       'statusCode': statusCode,
       'body': json.dumps(message)
   }