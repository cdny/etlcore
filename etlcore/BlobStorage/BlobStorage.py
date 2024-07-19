import io
from azure.storage.blob import BlobServiceClient


class BlobStorage:

    def __init__(self, connection_string: str) -> None:

        try:
            self.service = BlobServiceClient.from_connection_string(
                conn_str=connection_string)
        except:
            print("The connection to blob storage failed")

    def get_file(self, file_path: str, container_name):
        
        blob_client = self.service.get_blob_client(container_name, file_path)
        stream = io.BytesIO()
        blob_client.download_blob().readinto(stream)
        
        return stream
