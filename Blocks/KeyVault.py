import os

from prefect.blocks.core import Block
from prefect.blocks.system import Secret, String

from azure.keyvault.secrets import SecretClient
from azure.identity import DefaultAzureCredential

from pydantic import SecretStr

class KeyVault(Block):
    azure_client_id: SecretStr
    azure_client_secret: SecretStr
    azure_tenant_id: SecretStr
    vault_url: str

    """Connect to Azure Key Vault and get all the keys or one key.
    """

    _block_type_name="Azure Key Vault"
    _block_type_slug="azure-key-vault"
    _logo_url="https://images.ctfassets.net/gm98wzqotmnx/6AiQ6HRIft8TspZH7AfyZg/39fd82bdbb186db85560f688746c8cdd/azure.png?h=250"
    # _description	Short description of block type. Defaults to docstring, if provided.
    # _code_example


    def __init__(self) -> None:
        super().__init__()

        os.environ["AZURE_CLIENT_ID"] = self.azure_client_id
        os.environ["AZURE_CLIENT_SECRET"] = self.azure_client_secret
        os.environ["AZURE_TENANT_ID"] = self.azure_tenant_id
        
        credential = DefaultAzureCredential()
        self.client = SecretClient(vault_url=self.vault_url, credential=credential)


    def get_secrets(self) -> dict:

        secrets = self.client.list_properties_of_secrets()
        result = dict()

        for s in secrets:
            sec = self.client.get_secret(s.name)
            sec_name = sec.name.replace("-", "_")
            result.update({sec_name: sec.value})

        return result


