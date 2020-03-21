from pslx.micro_service.instant_messaging.client import SlackClient, RocketchatClient, TeamsClient

if __name__ == "__main__":
    server_url = "localhost:11443"
    message = "hello world"
    slack_client = SlackClient(
        channel_name='staging_test',
        webhook_url='https://hooks.slack.com/services/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',
        server_url=server_url
    )
    slack_client.send_message(message=message, is_test=False)

    rocketchat_client = RocketchatClient(
        channel_name='internal_msg_queue',
        webhook_url='http://165.227.12.178:3000/hooks/'
                     '7YDoDrqcsyHHqRtHd/6YnK7GbDgzit38DwhH2TppRG6turXNdJ24JsbPyJhy28E6JG',
        server_url=server_url
    )
    rocketchat_client.send_message(message=message, is_test=False)

    teams_client = TeamsClient(
        channel_name='internal_msg_queue',
        webhook_url="https://outlook.office.com/webhook/"
                    "82c18bf1-2994-403d-888d-f11343682d72@ec60084f-c956-4c79-94a1-a3243cf2eea8/"
                    "IncomingWebhook/4880528f42434ef7827809b2c403c7f6/623b9f83-e4f8-4ae2-946a-710a6d4e085e",
        server_url=server_url
    )
    teams_client.send_message(message=message, is_test=False)
