import json
import requests
import pandas as pd
import os

def get_current_metar():
  endpoint = 'https://api-redemet.decea.mil.br/mensagens/metar/SBBI,SBCT'
  API_KEY = os.environ.get('API_KEY')
  data = requests.get(url=endpoint, params={'API_KEY':API_KEY})
  if data.status_code == 200:
    parsed_json = json.loads(data.text)
    metar_json_array = parsed_json['data']['data']
    return metar_json_array


def get_and_load_metar(event, context):
  df = pd.DataFrame(get_current_metar())
  df['validade_inicial'] = pd.to_datetime(df['validade_inicial'])
  df['recebimento'] = pd.to_datetime(df['recebimento'])
  df.columns = ['id_location', 'dh_expiration', 'metar_code', 'dh_request']
  df.to_gbq(destination_table='api_data.tb_brl_metar',
            project_id='geometric-team-354101',
            if_exists='append',
            table_schema=[{'name': 'id_location', 'type': 'STRING'},
                          {'name': 'dh_expiration', 'type': 'TIMESTAMP'},
                          {'name': 'metar_code', 'type': 'STRING'},
                          {'name': 'dh_request', 'type': 'TIMESTAMP'}]
                          )