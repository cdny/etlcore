import io, os
import pysftp
from paramiko.hostkeys import HostKeys

class SFTP():
    def __init__(self, host: str, username: str, password: str, hostkeys: HostKeys = None, port: int = 22):
        self.host = host
        self.username = username
        self.password = password
        self.hostkeys = hostkeys
        self.port = port
        self.connection = self._create_sftp_client()
        print(f"Connected to {self.host} as {self.username}")

    def _create_sftp_client(self):
        try:
            cnopts = pysftp.CnOpts()
            cnopts.hostkeys = self.hostkeys

            return pysftp.Connection(
                host = self.host,
                username = self.username,
                password = self.password,
                cnopts=cnopts,
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

    def list_dir_contents(self, remote_path) -> set:
        try:
            files = set([file for file in self.connection.listdir(remote_path)]) #set because it should not change
            return files
        except Exception as e:
            return f"Failed to list out files from directory: {str(e)}"
        
    def list_dir_attributes(self, remote_path) -> set:
        try:
            files = set([file for file in self.connection.listdir_attr(remote_path)]) #set because it should not change
            return files
        except Exception as e:
            return f"Failed to list out files from directory: {str(e)}"

    def upload_file(self, local_file_path: str, remote_path: str) -> bool:
        try:
            filename = local_file_path #need to strip local_file_path down to just the filename
            if "/" in filename:
                filename = filename.rsplit('/',1)[1] #get just the filename if a full dir path is given
            
            remote_path_full = os.path.join(remote_path, filename) #need to specify full path and filename on sftp server for put operation
            self.connection.put(local_file_path, remote_path_full)
            print(f"File uploaded successfully to '{self.host}' in location '{remote_path}'.")
            return True
        except Exception as e:
            return f"Failed to upload file: {str(e)}"

    #this function assumes you have no sub folders or folders of any kind in the directory
    #while this IS our use case, if you worry about folders being added use "create_and_upload_dir()" instead
    def upload_dir_contents(self, local_dir_path: str, remote_dir_path: str) -> bool:
        try:
            for file in os.listdir(local_dir_path): #iterate over files in directory
                remote_path = os.path.join(remote_dir_path, file)
                local_path = os.path.join(local_dir_path, file)
                self.connection.put(local_path, remote_path)
            return True
        except Exception as e:
            return f"Failed to upload directory contents: {str(e)}"
        
    #creates a directory if none exists, needs to be passed full remote dir path  
    def create_dir(self, remote_dir_path: str) -> bool:
        try: 
            self.connection.chdir(remote_dir_path) #see if remote path exists
        except IOError:
            self.connection.mkdir(remote_dir_path) #create path if it doesnt
            return True


    def create_and_upload_dir(self, local_dir_path: str, remote_dir_path: str) -> bool:
        try:
            dir_name = local_dir_path 
            if "/" in dir_name:
                dir_name = local_dir_path.rsplit('/',1)[1]
            remote_dir_to_create = os.path.join(remote_dir_path, dir_name)
            self.create_dir(remote_dir_to_create) #create new remote directory in desired location

            for file in os.listdir(local_dir_path): #iterate over files in starting directory
                remote_path = os.path.join(remote_dir_to_create, file) 
                local_path = os.path.join(local_dir_path, file)
                self.connection.put(local_path, remote_path)
                print(f"file created at {remote_path}")
            return True
        except Exception as e:
            return f"Failed to upload directory: {str(e)}"

    def download_file(self, remote_path: str, local_file_path: str) -> bool:
        try:
            path, _ = os.path.split(local_file_path)
            if not os.path.isdir(local_file_path) and path != '': #if path does not exist, create it. skip if its just the file
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
    def download_dir_contents(self, remote_dir_path: str, local_dir_path: str) -> bool:
        try:
            for file in self.connection.listdir_attr(remote_dir_path): #iterate over files in directory
                remote_path = os.path.join(remote_dir_path, file.filename) 
                local_path = os.path.join(local_dir_path, file.filename)
                self.connection.get(remote_path, local_path)
            return True
        except Exception as e:
            return f"Failed to upload directory contents: {str(e)}"

    #https://stackoverflow.com/questions/50118919/python-pysftp-get-r-from-linux-works-fine-on-linux-but-not-on-windows    
    def download_full_dir_contents(self, remote_dir_path: str, local_dir_path: str) -> bool:
        try:
            dir_name = remote_dir_path 
            if "/" in dir_name:
                dir_name = local_dir_path.rsplit('/',1)[1]
            local_dir_to_create = os.path.join(local_dir_path, dir_name)
            try:
                os.makedirs(local_dir_to_create)
            except Exception as e:
                return f"Failed to create new directory for downloaded file: {str(e)}"  
            for file in self.connection.listdir_attr(remote_dir_path): #iterate over files in starting directory
                remote_path = os.path.join(remote_dir_path, file.filename) 
                local_path = os.path.join(local_dir_to_create, file.filename)
                self.connection.get(remote_path, local_path)
            return True
        except Exception as e:
            return f"Failed to upload directory: {str(e)}"


    def delete_file(self, remote_file_path: str) -> bool:
        try:
            self.connection.remove(remote_file_path)
            print(f"File '{remote_file_path}' deleted successfully from SFTP Host '{self.host}'.")
            return True
        except Exception as e:
            return f"Failed to delete file: {str(e)}"

    def rename_file(self, remote_file_path: str, new_filename: str) -> bool:
        try:
            if "/" in remote_file_path:
                filepath_to_keep = remote_file_path.rsplit('/',1)[0] #get filepath to the file you want to rename
                new_remote_filepath = os.path.join(filepath_to_keep, new_filename) #get the full path with new filename attached
                self.connection.rename(remote_file_path, new_remote_filepath) #rename existing path to new path
                print(f"File '{remote_file_path}' renamed successfully to '{new_remote_filepath}'.")
            else:
                self.connection.rename(remote_file_path, new_filename) #if its root directories then nobody cares
            return True
        except Exception as e:
            return f"Failed to rename file: {str(e)}"

