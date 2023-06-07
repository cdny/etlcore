import io
import boto3
import pandas as pd
class S3():
    def __init__(self, access_key: str, secret_key: str, bucket_name: str):
        self.access_key = access_key
        self.secret_key = secret_key
        self.bucket_name = bucket_name
        self.s3_client = self._create_s3_client()

    def _create_s3_client(self):
        try:
            return boto3.client(
                "s3",
                aws_access_key_id=self.access_key,
                aws_secret_access_key=self.secret_key,
            )
        except Exception as e:
            print(f"Failed to create s3 client: {str(e)}")

    def list_files(self):
        try:
            response = self.s3_client.list_objects_v2(Bucket=self.bucket_name) #https://awscli.amazonaws.com/v2/documentation/api/latest/reference/s3api/list-objects-v2.html
            return response
        except Exception as e:
            print(f"Failed to list out Contents of s3 bucket: {str(e)}")

    def upload_file(self, s3_key: str, local_file_path: str):
        try:
            self.s3_client.upload_file(local_file_path, self.bucket_name, s3_key)
            print(
                f"File uploaded successfully to S3 bucket '{self.bucket_name}' with key '{s3_key}'."
            )
            return s3_key
        except Exception as e:
            print(f"Failed to upload file: {str(e)}")

    def download_file(self, s3_key: str, local_file_path: str):
        try:
            self.s3_client.download_file(self.bucket_name, s3_key, local_file_path)
            print(
                f"File downloaded successfully from S3 bucket '{self.bucket_name}' with key '{s3_key}'."
            )
            #TO DO: RETURN FILE PATH?
        except Exception as e:
            print(f"Failed to download file: {str(e)}")


    def delete_file(self, s3_key: str) -> bool:
        try:
            self.s3_client.delete_object(Bucket=self.bucket_name, Key=s3_key)
            print(
                f"File '{s3_key}' deleted successfully from S3 bucket '{self.bucket_name}'."
            )
            return True
        except Exception as e:
            print(f"Failed to delete file: {str(e)}")

    def rename_file(self, s3_key: str, new_s3_key: str) -> bool:
        try:
            copy_source = {'Bucket': self.bucket_name, 'Key': s3_key} # you cannot rename objects in s3, so you need to copy to a new name and then delete the old one
            self.s3_client.copy_object(CopySource = copy_source, Bucket = self.bucket_name, Key = new_s3_key)
            self.s3_client.delete_object(Bucket = self.bucket_name, Key = s3_key)
            print(
                f"File '{s3_key}' renamed successfully to '{new_s3_key}'."
            )
            return True
        except Exception as e:
            print(f"Failed to rename file: {str(e)}")

    def get_file_content(self, s3_key: str) -> pd.DataFrame:
        try:
            response = self.s3_client.get_object(Bucket=self.bucket_name, Key=s3_key)
            chunks = (
                chunk for chunk in response["Body"].iter_chunks(chunk_size=1024**3)
            )  # This should prevent the 2GB download limit from a python ssl internal
            data = io.BytesIO(b"".join(chunks))  # This keeps everything fully in memory
            file_type = s3_key.split('.')[1]
            match file_type:
                case "csv":
                    df = pd.read_csv(data)  # here you can provide also some necessary args and kwargs
                    return df
                case "xlsx":
                    df = pd.read_excel(data)
                    return df
                case other:
                    raise NotImplementedError("File type provided is not yet implemented!")
        except Exception as e:
            print(f"Failed to get file content: {str(e)}")