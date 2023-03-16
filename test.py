from Blocks.KeyVault import KeyVault

azure_key_vault_block = KeyVault.load("cdny-key-vault")


azure_key_vault_block.get_secrets()