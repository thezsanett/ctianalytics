from OTXv2 import OTXv2, IndicatorTypes
from kafka import KafkaProducer
import os
import time
import json
from datetime import datetime, timedelta

SLEEP_TIME = int(os.environ.get("SLEEP_TIME", 5))
producer = KafkaProducer(bootstrap_servers=["broker:9092"], api_version=(0, 10, 1))

# otx = OTXv2("a1c6e949d849b28592e0f25ebbd6d05c4cb49d28f442c96f45d5342874e4c286")
otx = None

def json_serializer(data):
  return json.dumps(data).encode("utf-8")

def get_and_send_pulse(otx, pulse_id):
  # indicators = otx.get_pulse_indicators("602bc528f447d628d41494f2", include_inactive=False)
  indicators = [
      {'indicator': '69.73.130.198', 'type': 'IPv4'},
      {'indicator': 'aoldaily.com', 'type': 'Domain'}
  ]

  for indicator in indicators:
    # data = otx.get_indicator_details_full(IndicatorTypes.DOMAIN, indicator["indicator"])
    data = indicator

    producer.send('alienvaultdata', json_serializer(data))

    print("Sent: ", row)
    time.sleep(5)


get_and_send_pulse(otx, "602bc528f447d628d41494f2")
# get_and_send_pulse(otx, "") #TODO
