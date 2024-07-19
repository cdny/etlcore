from azure.storage.blob import BlobServiceClient


class BlobStorage:

    def __init__(self, connection_string: str) -> None:

        try:
            service = BlobServiceClient.from_connection_string(
                conn_str=connection_string)
        except:
            print("There was an issue connecting to Blob Storage")
