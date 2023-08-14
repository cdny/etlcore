import os
from office365.runtime.auth.client_credential import ClientCredential
from office365.runtime.auth.user_credential import UserCredential
from office365.sharepoint.client_context import ClientContext

class Sharepoint():
    def __init__(self, auth_type: str, credentials: UserCredential | ClientCredential, site: str):
        
        self.auth_type = auth_type
        self.credentials = credentials
        self.site = site
        self.sp_ctx_connection = self._create_sharepoint_connection()
        print(f"Connected to Sharepoint using {self.auth_type}")

    def _create_sharepoint_connection(self):
        try:
            self.sp_ctx_connection = ClientContext(self.site).with_credentials(self.credentials)
            return self.sp_ctx_connection
        except Exception as e:
            return f"Failed to create sharepoint connection object: {str(e)}"

    def upload_file_to_sharepoint(self, sp_folder: str, local_file_path: str):
        try:
            with open(local_file_path, "rb") as content_file:
                file_content = content_file.read()
            filename = os.path.basename(local_file_path)
            target_folder = self.sp_ctx_connection.web.get_folder_by_server_relative_url(sp_folder)
            target_folder.upload_file(filename, file_content)
            self.sp_ctx_connection.execute_query()
            return True
        except Exception as e:
            return f"Failed to upload file {local_file_path} to sharepoint: {str(e)}"
        
    def download_file_from_sharepoint(self, sp_folder: str, remote_filename: str, local_file_path: str):
        try:
            remote_path = os.path.join(sp_folder, remote_filename) #file on sharepoint
            destination_path = os.path.join(local_file_path, os.path.basename(remote_filename)) #destination location for file
            with open(destination_path, "wb") as local_file:
                file = self.sp_ctx_connection.web.get_file_by_server_relative_url(remote_path).download(local_file).execute_query()
                return True
        except Exception as e:
            return f"Failed to download file {remote_filename} from {sp_folder} to {local_file_path}"
        
    def get_folder_contents(self, relative_url):
        try:
            libraryRoot = self.sp_ctx_connection.web.get_folder_by_server_relative_url(relative_url)
            self.sp_ctx_connection.load(libraryRoot)
            self.sp_ctx_connection.execute_query()
            
            files = libraryRoot.files
            self.sp_ctx_connection.load(files)
            self.sp_ctx_connection.execute_query()

            file_list = []
            for myfile in files:
                print("File name: {0}".format(myfile.properties["ServerRelativeUrl"]))
                pathList = myfile.properties["ServerRelativeUrl"].split('/')
                file_list +=[pathList[-1]]
                #fileDest = outputDir + "/"+ pathList[-1]
                #downloadFile(self.sp_ctx_connection, fileDest, myfile.properties["ServerRelativeUrl"])
            return file_list
        except Exception as e:
            print(f'Problem printing out list of folders {str(e)}')