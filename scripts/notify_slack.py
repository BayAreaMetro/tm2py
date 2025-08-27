USAGE = """

Notify slack of model run status

"""

import argparse, os, socket, sys
import json,requests

def post_message(message):
    """
    Posts the given message to the slack channel via the webhook if SLACK_WEBHOOK_URL
    Also prints to console
    """
    hostname = socket.getfqdn()
    instance = os.environ.get('INSTANCE')
    if not instance:
        print("ERROR: The INSTANCE environment variable is not set. Slack notification will not be sent.")
        instance = "UNKNOWN"

    if hostname.endswith(".mtc.ca.gov"):
        SLACK_WEBHOOK_URL_FILE = r"M:\\Software\\Slack\\TravelModel_SlackWebhook.txt"
        print(f"Running on mtc host; using {SLACK_WEBHOOK_URL_FILE}")
    else:
        SLACK_WEBHOOK_URL_FILE = r"C:\\Software\\Slack\\TravelModel_SlackWebhook.txt"
        print(f"Running on non-mtc host; using {SLACK_WEBHOOK_URL_FILE}")

    SLACK_WEBHOOK_URL = None
    try:
        with open(SLACK_WEBHOOK_URL_FILE, "r") as f:
            SLACK_WEBHOOK_URL = f.read().strip()
        print(f"Read slack webhook URL: {SLACK_WEBHOOK_URL}")
    except Exception as e:
        print(f"ERROR: Could not read Slack webhook file: {SLACK_WEBHOOK_URL_FILE}. {e}")
        SLACK_WEBHOOK_URL = None

    full_message = f"*{instance}*: {message}"
    headers = { 'Content-type':'application/json'}
    data = { "text": full_message }
    response = None
    if SLACK_WEBHOOK_URL:
        try:
            response = requests.post(SLACK_WEBHOOK_URL, headers=headers, json=data)
            print(f"response: {response}")
        except Exception as e:
            print(f"ERROR: Failed to send Slack message: {e}")
    else:
        print("ERROR: Slack webhook URL not set. Message not sent to Slack.")

    print(f"*** {full_message}")

if __name__ == '__main__':

    parser = argparse.ArgumentParser(description=USAGE, formatter_class=argparse.RawDescriptionHelpFormatter,)
    parser.add_argument("message", type=str, help="The message to send")
    args = parser.parse_args()

    post_message(args.message)
    # that's all