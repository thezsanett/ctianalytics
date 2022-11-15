from OTXv2 import OTXv2, IndicatorTypes
from kafka import KafkaProducer

import json
from datetime import datetime, timedelta

otx = OTXv2("a1c6e949d849b28592e0f25ebbd6d05c4cb49d28f442c96f45d5342874e4c286")

indicators = otx.get_pulse_indicators("602bc528f447d628d41494f2", include_inactive=True)

#TODO: get more data like this?
#otx.get_indicator_details_full(IndicatorTypes.DOMAIN, "knucker.io")


def json_serializer(data):
    return json.dumps(data).encode("utf-8")

producer = KafkaProducer(bootstrap_servers=['broker:9092'],  value_serializer=json_serializer)
for row in indicators:
  producer.send('alienvaultdata', value=row)
  print("Sent: ", row)

