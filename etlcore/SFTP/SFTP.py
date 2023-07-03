import io, os
import pysftp
import pandas as pd
class SFTP():
    def __init__(self, host: str, username: str, password: str, port: int = 22):
        self.host = host
        self.username = username
        self.password = password
        self.port = port
        self.connection = self._create_sftp_client()
        print(f"Connected to {self.host} as {self.username}")

    def _create_sftp_client(self):
        try:
            return pysftp.Connection(
                host = self.host,
                username = self.username,
                password = self.password,
                port = self.port
            )
        except Exception as e:
            return f"Failed to create sftp client object: {str(e)}"
        
    def disconnect(self): 
        try:
            self.connection.close()
            print(f"Disconnected from host {self.host}")
            return True
        except Exception as e:
            return f"failed to disconnect from host {self.host}"

    def list_files(self, remote_path):
        try:
            for obj in self.connection.listdir(remote_path):
                yield obj
        except Exception as e:
            return f"Failed to list out files from directory: {str(e)}"
        
    def list_file_attributes(self, remote_path):
        try:
            for attributes in self.connection.listdir_attr(remote_path):
                yield attributes
        except Exception as e:
            return f"Failed to list out files from directory: {str(e)}"

    def upload_file(self, local_file_path: str, remote_path: str):
        try:
            self.connection.put(local_file_path, remote_path)
            print(
                f"File uploaded successfully to '{self.host}' in location '{remote_path}'."
            )
            return True
        except Exception as e:
            return f"Failed to upload file: {str(e)}"

    def download_file(self, remote_path: str, local_file_path: str):
        try:
            path, _ = os.path.split(local_file_path)
            if not os.path.isdir(local_file_path):
                try:
                    os.makedirs(path)
                except Exception as e:
                    return f"Failed to create new directory for downloaded file: {str(e)}"              
            self.connection.get(remote_path, local_file_path)
            print(f"File downloaded successfully from host '{self.host}' to '{local_file_path}'.")
            return True
        except Exception as e:
           return f"Failed to download file: {str(e)}"


    def delete_file(self, filename: str) -> bool:
        try:
            #TODO
            print(f"File '{filename}' deleted successfully from SFTP Host '{self.host}'.")
            return True
        except Exception as e:
            return f"Failed to delete file: {str(e)}"

    def rename_file(self, filename: str, new_filename: str) -> bool:
        try:
            #TODO
            print(f"File '{filename}' renamed successfully to '{new_filename}'.")
            return True
        except Exception as e:
            return f"Failed to rename file: {str(e)}"

