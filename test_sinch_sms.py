#!/home/ec2-user/.local/share/virtualenvs/watchlist-dO_9O89D/bin/python3
from sinch import Client

sinch_client = Client(
    key_id="1ec91efb-a6cd-4383-ad90-7c9c58e891bf",
    key_secret=".NnnJHQ8rWIpf5EZlJV7B~t4YD",
    project_id="502c300f-443c-4b85-a7e5-24f8d6e6fb97"
)

for i in range(5):
    send_batch_response = sinch_client.sms.batches.send(
        body="healy max red candle alert! " + str(i),
        to=["+19014830859"],
        from_="+12085812142",
        delivery_report="none"
    )

print(send_batch_response)


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
