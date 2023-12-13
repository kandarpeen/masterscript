import sys
import os
import boto3
import json

ACCESS_KEY = "AKIA375DJFVNSMV2FQOT"
SECRET_KEY="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"

def upload_images_from_folder(bucket_name, folder_path, num_images , s3):
    """
    Uploads specified number of images from a folder to an AWS S3 bucket.

 
    :param folder_path: The local path to the folder containing the images.
  
    """

    upload_file :str= []



    for root, _, files in os.walk(folder_path):
        for filename in files[:num_images]:
            local_image_path = os.path.join(root, filename)
            s3_key = os.path.relpath(local_image_path, folder_path)
            s3.upload_file(local_image_path, bucket_name, s3_key)
            url = f"https://{bucket_name}.s3.amazonaws.com/{s3_key}"
            upload_file.append(url)
    
    with open("url.json", 'w') as json_file:
        json.dump({"data":upload_file}, json_file)



if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python upload_images.py  <folder_path> <num_images>")
        sys.exit(1)

    s3 = boto3.client('s3', aws_access_key_id=ACCESS_KEY,
                      aws_secret_access_key=SECRET_KEY)
    

    bucket_name = "checking-11"
    folder_path = sys.argv[1]
    num_images = int(sys.argv[2])

    upload_images_from_folder(bucket_name, folder_path, num_images, s3)
    print(f"{num_images} images uploaded to {bucket_name} successfully.")
