import ovh 
import os 
import pandas as pd 
from dotenv import load_dotenv
load_dotenv()

# determine tes credentials
endpoint = os.getenv('endpoint')
application_key = os.getenv('application_key')
application_secret = os.getenv('application_secret')
consumer_key = os.getenv('consumer_key')
params = { 'from': '2021-01-01T12:00:00+01:00', 'to': '2023-01-01T12:00:00+01:00' }
serviceName = 'dca527889f174efd8f77b899039e5176'

# Client ovh 
client = ovh.Client(
    endpoint=endpoint,
    application_key=application_key,
    application_secret=application_secret,
    consumer_key=consumer_key )

# requests for services id 
usages = client.get(f'/cloud/project/{serviceName}/usage/history', **params)

# create dataframe 
df = pd.DataFrame(usages)

# requests for each service 
df['hourlyUsage'] = df.id.apply(lambda x: client.get(f'/cloud/project/{serviceName}/usage/history/{x}', **params) )

# iterate in hourly Usage and Period
df['period'] = df.hourlyUsage.apply(lambda x: x['period'])
df['hourlyUsage'] = df.hourlyUsage.apply(lambda x: x['hourlyUsage'])
df['service_instance'] = df.hourlyUsage.apply(lambda x: x['instance'])
df['service_volume'] = df.hourlyUsage.apply(lambda x: x['volume'])
df['service_snapshot'] = df.hourlyUsage.apply(lambda x: x['snapshot'])
df['from'] = df.period.apply(lambda x: x['from'])
df['to'] = df.period.apply(lambda x: x['to'])
df['from'] = df['from'].apply(lambda x: pd.to_datetime(x))
df['to'] = df['to'].apply(lambda x: pd.to_datetime(x))
df['diff'] = df['to'] - df['from']
df['diff'] = df['diff'].astype('timedelta64[D]')

# On reparti en 3 dataframe les services: 
df = df.explode('service_instance').explode('service_volume').explode('service_snapshot')
df_instance = df[['id','from','to','service_instance','diff']].copy().rename(columns={"service_instance": "service"})
df_volume = df[['id','from','to','service_volume','diff']].copy().rename(columns={"service_volume": "service"})
df_snapshot = df[['id','from','to','service_snapshot','diff']].copy().rename(columns={"service_snapshot": "service"})

# datafram for instance service: 
df_instance['region'] = df_instance['service'].apply(lambda x: x['region'] if isinstance(x, dict) else x )
df_instance['resource_id'] = df_instance['service'].apply(lambda x: x['details'][0]['instanceId'] if isinstance(x, dict) else None)
df_instance['resource_path'] = df_instance['service'].apply(lambda x: x['details'][0]['instanceId'] if isinstance(x, dict) else None)
df_instance['usage_type'] = df_instance['service'].apply(lambda x: x['quantity']['unit'] + ' - ' + x['reference'] if isinstance(x, dict) else None)
df_instance['usage_quantity'] = df_instance['service'].apply(lambda x: x['quantity']['value'] if isinstance(x, dict) else None)
df_instance['usage_unit'] = df_instance['service'].apply(lambda x: x['quantity']['unit'] if isinstance(x, dict) else None)
df_instance['cost_total'] = df_instance['service'].apply(lambda x: x['details'][0]['totalPrice'] if isinstance(x, dict) else None)
df_instance['cost'] = df_instance['cost_total'] /  df_instance['diff']
df_instance = df_instance.drop(['service','diff'], axis=1).drop_duplicates()

# datafram for volume service:
df_volume['region'] = df_volume['service'].apply(lambda x: x['region'] )
df_volume['resource_id'] = df_volume['service'].apply(lambda x: x['details'][0]['volumeId'])
df_volume['resource_path'] = df_volume['service'].apply(lambda x: x['details'][0]['volumeId'])
df_volume['usage_type'] = df_volume['service'].apply(lambda x: x['quantity']['unit'] + ' - ' + x['type']) # replace type by reference 
df_volume['usage_quantity'] = df_volume['service'].apply(lambda x: x['quantity']['value'] )
df_volume['usage_unit'] = df_volume['service'].apply(lambda x: x['quantity']['unit'])
df_volume['cost_total'] = df_volume['service'].apply(lambda x: x['details'][0]['totalPrice'])
df_volume['cost'] = df_volume['cost_total'] /  df_volume['diff']
df_volume = df_volume.drop(['service','diff'], axis=1).drop_duplicates()

# dataframe for snapshot service 
df_snapshot['region'] = df_snapshot['service'].apply(lambda x: x['region'] )
df_snapshot['resource_id'] = "resource_id"
df_snapshot['resource_path'] = "resource_path"
df_snapshot['usage_type'] = df_snapshot['service'].apply(lambda x: x['instance']['quantity']['unit'] + ' - ' ) # Need to defined a reference snapshots 
df_snapshot['usage_quantity'] = df_snapshot['service'].apply(lambda x: x['instance']['quantity']['value'] )
df_snapshot['usage_unit'] = df_snapshot['service'].apply(lambda x: x['instance']['quantity']['unit'])
df_snapshot['cost_total'] = df_snapshot['service'].apply(lambda x: x['instance']['totalPrice'])
df_snapshot['cost'] = df_snapshot['cost_total'] /  df_snapshot['diff']
df_snapshot = df_snapshot.drop(['service','diff'], axis=1).drop_duplicates()

# Insert service type in each dataFrame
df_instance['service'] = 'instance'
df_volume['service'] = 'volume'
df_snapshot['service'] = 'snapshot'

# Merge final datafram and create csv file
df_final = pd.concat([df_instance, df_volume, df_snapshot]).sort_values('from')
df_final['provider'] = "OVHCLOUD"
df_final['product'] = "PUBLIC CLOUD"
df_final['currency'] = 'EUR'
df_final = df_final[['id','from', 'to', 'provider', 'product', 'region','service','resource_id', 'resource_path', 'usage_quantity','usage_unit','usage_type','cost','currency']]

df_final.to_csv('bill_script.csv', index=False)