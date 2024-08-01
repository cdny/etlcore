import io
from azure.storage.blob import BlobServiceClient


class BlobStorage:

    def __init__(self, connection_string: str) -> None:

        try:
            self.blob_service_client = BlobServiceClient.from_connection_string(
                conn_str=connection_string)
        except:
            print("The connection to blob storage failed")

    def get_file(self, file_path: str, container_name: str):

        blob_client = self.blob_service_client.get_blob_client(
            container_name, file_path)
        stream = io.BytesIO()
        blob_client.download_blob().readinto(stream)

        return stream

    def delete_blob(self, container_name: str, blob_name: str):

        blob_client = self.blob_service_client.get_blob_client(
            container_name, blob_name)
        blob_client.delete_blob()

    def upload_blob(self, container_name: str, file_path: str, overwrite: bool = False):

        blob_client = self.blob_service_client.get_blob_client(
            container_name, file_path
        )
        with open(file_path, "rb") as file_content:
            blob_client.upload_blob(file_content.read(), overwrite=overwrite)
