#!/home/ec2-user/.local/share/virtualenvs/watchlist-dO_9O89D/bin/python3
from sinch import Client
from sinch_numbers import numbers
from configparser import ConfigParser
import os

config_object = ConfigParser()
base_path = os.path.dirname(os.path.realpath(__file__))
config_read_path = config_object.read(os.path.join(base_path, "config.ini"))
config_object.read(config_read_path)
DEBUG_ALERTS = config_object['main']['DEBUG_ALERTS']

sinch_client = Client(
    key_id="1ec91efb-a6cd-4383-ad90-7c9c58e891bf",
    key_secret=".NnnJHQ8rWIpf5EZlJV7B~t4YD",
    project_id="502c300f-443c-4b85-a7e5-24f8d6e6fb97"
)

# for i in range(1):
#     send_batch_response = sinch_client.sms.batches.send(
#         body="healy max red candle alert! " + str(i),
#         to=["+19014830859"],
#         from_="+12085812142",
#         delivery_report="none"
#     )

sinch_numbers_list = list(numbers.values())
if DEBUG_ALERTS == 'True':
    sinch_numbers_list = [numbers['healy']]
def send_sms_alert(alert_category, ticker, price, fundamentals=None):
    send_batch_response = sinch_client.sms.batches.send(
        body="healy's alert:" "\n" + alert_category + "\n" + ticker + "\n" + str(price),
        to=sinch_numbers_list,
        from_="+12085812142",
        delivery_report="none"
    )


#import requests
#
#servicePlanId = "105fcc3dd26248a28bc6c47bb21228dc"
#apiToken = "e1d7f275be4d4d8690fc6f99fc01c673"
#sinchNumber = "+12085812142"
#toNumber = "+90114830859"
#url = "https://us.sms.api.sinch.com/xms/v1/" + servicePlanId + "/batches"
#
#payload = {
#  "from": sinchNumber,
#  "to": [
#    toNumber
#  ],
#  "body": "Hello how are you"
#}
#
#headers = {
#  "Content-Type": "application/json",
#  "Authorization": "Bearer " + apiToken
#}
#
#response = requests.post(url, json=payload, headers=headers)
#
#data = response.json()
#print(data)
