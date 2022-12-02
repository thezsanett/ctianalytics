from OTXv2 import OTXv2, IndicatorTypes
from kafka import KafkaProducer
from datetime import datetime
import uuid, json

producer = KafkaProducer(bootstrap_servers=["broker:9092"], api_version=(0, 10, 1))
empty_detail = {
  'geo': {
    'latitude': -1,
    'longitude': -1,
    'accuracy_radius': -1,
  },
  'url_list': {
    'url_list': []
  }
}

pulses = [
  "602bc528f447d628d41494f2",
  "617b089bbbab1cd42903ced7"
]

otx = OTXv2("a1c6e949d849b28592e0f25ebbd6d05c4cb49d28f442c96f45d5342874e4c286")

def get_details(indicator):
  try:
    if indicator['type'] == 'IPv4':
      return otx.get_indicator_details_full(IndicatorTypes.IPv4, indicator["indicator"])
    if indicator['type'] == 'Domain':
      return otx.get_indicator_details_full(IndicatorTypes.DOMAIN, indicator["indicator"])
    return empty_detail
  except:
    print('Detail for pulse ' + indicator['indicator'] + ' can not be found.')
    return empty_detail

def json_serializer(data):
  return json.dumps(data).encode("utf-8")

def get_and_send_pulse(otx, pulse_id, generated_id):
  indicators = otx.get_pulse_indicators(pulse_id, include_inactive=False)
  # Uncomment the line below for testing purposes:
  # indicators = [
  #   {'indicator': '69.73.130.198', 'type': 'IPv4'},
  #   {'indicator': 'aoldaily.com', 'type': 'Domain'}
  # ]

  print("Pulse acquired.")

  for data in indicators:
    details = get_details(data)
    data['latitude'] = int(details['geo']['latitude']) if 'latitude' in details['geo'] else -1
    data['longitude'] = int(details['geo']['longitude']) if 'longitude' in details['geo'] else -1
    data['accuracy_radius'] = details['geo']['accuracy_radius'] if 'accuracy_radius' in details['geo'] else -1
    data['url_list_length'] = len(details['url_list']['url_list'])
    data['pulse_id'] = generated_id
    data['created'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    producer.send('alienvaultdata', json_serializer(data))

    print("\nSent: ", data)

for pulse in pulses:
  generated_id = str(uuid.uuid1())
  get_and_send_pulse(otx, pulse, generated_id)
