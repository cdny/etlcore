from prefect.blocks.core import Block

import requests


class Email(Block):
    """Send emails using the CDNY API."""

    _block_type_name:str = "Email API"
    _block_type_slug:str = "email-api"
    _logo_url:str = "https://i.pravatar.cc/250"
    # _description	Short description of block type. Defaults to docstring, if provided.
    # _code_example

    def send_with_attachment(
        self, subject, to_recipients, send_as, attachment_data, attachment_filename
    ):
        s = requests.Session()
        s.headers.update(
            {
                "Content-Type": "application/json",
            }
        )
        jsonx = {
            "subject": subject,
            "to_recipients": to_recipients,
            "send_as": send_as,
            "attachments": [
                {"content_bytes": attachment_data, "filename": attachment_filename}
            ],
        }
        resp = s.post(url="https://cdny-api.azurewebsites.net/email/", json=jsonx)
        del resp
