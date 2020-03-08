from pslx.micro_service.email.client import EmailClient


if __name__ == "__main__":
    server_url = "165.227.12.177:11443"
    email_client = EmailClient(server_url=server_url)
    email_client.send_email(
        from_email='alphahunter2019@gmail.com',
        to_email='kfrancischen@gmail.com',
        content='this is a test.'
    )
