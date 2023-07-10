import io, os
import pysftp
import pandas as pd
from stat import S_ISDIR, S_ISREG

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
        
    def disconnect(self) -> bool: 
        try:
            self.connection.close()
            print(f"Disconnected from host {self.host}")
            return True
        except Exception as e:
            return f"failed to disconnect from host {self.host}"

    def list_files(self, remote_path) -> set:
        try:
            files = set([file for file in self.connection.listdir(remote_path)]) #set because it should not change
            return files
        except Exception as e:
            return f"Failed to list out files from directory: {str(e)}"
        
    def list_file_attributes(self, remote_path) -> set:
        try:
            files = set([file for file in self.connection.listdir_attr(remote_path)]) #set because it should not change
            return files
        except Exception as e:
            return f"Failed to list out files from directory: {str(e)}"

    def upload_file(self, local_file_path: str, remote_path: str) -> bool:
        try:
            self.connection.put(local_file_path, remote_path)
            print(f"File uploaded successfully to '{self.host}' in location '{remote_path}'.")
            return True
        except Exception as e:
            return f"Failed to upload file: {str(e)}"

    #this function assumes you have no sub folders or folders of any kind in the directory
    #while this IS our use case, if you worry about folders being added use "create_and_upload_dir()" instead
    def upload_dir_contents(self, local_dir_path: str, remote_dir_path: str, preserve_mtime: bool = False) -> bool:
        try:
            for file in self.connection.listdir_attr(local_dir_path): #iterate over files in directory
                remote_path = remote_dir_path + "/" + file.filename 
                local_path = os.path.join(local_dir_path, file.filename)
                self.connection.put(local_path, remote_path, preserve_mtime)
            return True
        except Exception as e:
            return f"Failed to upload directory contents: {str(e)}"

    def create_and_upload_dir(self, local_dir_path: str, remote_dir_path: str, preserve_mtime: bool = False) -> bool:
        try:
            for file in self.connection.listdir_attr(local_dir_path): #iterate over files in starting directory
                remote_path = remote_dir_path + "/" + file.filename 
                local_path = os.path.join(local_dir_path, file.filename)
                mode = file.st_mode #identify whether file object is a folder or a file
                if S_ISDIR(mode): #returns true if the file is a directory
                    try:
                        os.mkdir(local_path)
                    except OSError as e:
                        print(f"unable to create directory: {str(e)}")
                        pass
                    self.create_and_upload_dir(local_path, remote_path, preserve_mtime) #recursive call to iterate down to the sub folder (where applicable)
                elif S_ISREG(mode): #returns true if the file is a file
                    self.connection.put(local_path, remote_path, preserve_mtime)
            return True
        except Exception as e:
            return f"Failed to upload directory: {str(e)}"

    def download_file(self, remote_path: str, local_file_path: str) -> bool:
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
        
    #this function assumes you have no sub folders or folders of any kind in the remote directory
    #while this IS our use case, if you worry about folders being added use "create_and_upload_dir()" instead
    def download_dir_contents(self, remote_dir_path: str, local_dir_path: str, preserve_mtime: bool = False) -> bool:
        try:
            for file in self.connection.listdir_attr(remote_dir_path): #iterate over files in directory
                remote_path = remote_dir_path + "/" + file.filename 
                local_path = os.path.join(local_dir_path, file.filename)
                self.connection.get(remote_path, local_path, preserve_mtime)
            return True
        except Exception as e:
            return f"Failed to upload directory contents: {str(e)}"

    #https://stackoverflow.com/questions/50118919/python-pysftp-get-r-from-linux-works-fine-on-linux-but-not-on-windows    
    def download_full_dir_contents(self, remote_dir_path: str, local_dir_path: str, preserve_mtime: bool = False) -> bool:
        try:
            for file in self.connection.listdir_attr(remote_dir_path): #iterate over files in starting directory
                remote_path = remote_dir_path + "/" + file.filename 
                local_path = os.path.join(local_dir_path, file.filename)
                mode = file.st_mode #identify whether file object is a folder or a file
                if S_ISDIR(mode): #returns true if the file is a directory
                    try:
                        os.mkdir(local_path)
                    except OSError as e:
                        print(f"unable to create directory: {str(e)}")
                        pass
                    self.download_full_dir_contents(remote_path, local_path, preserve_mtime) #recursive call to iterate down to the sub folder (where applicable)
                elif S_ISREG(mode): #returns true if the file is a file
                    self.connection.get(local_path, remote_path, preserve_mtime)
            return True
        except Exception as e:
            return f"Failed to upload directory: {str(e)}"


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

