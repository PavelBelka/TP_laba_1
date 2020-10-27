import requests
from sqlalchemy import create_engine, Table, Column, String, Float, MetaData
from sqlalchemy.sql import select

class WeatherProvider:
    def __init__(self, key):
        self.key = key

    def get_data(self, location, start_date, end_date):
        url = 'https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/weatherdata/history'
        params = {
            'aggregateHours': 24,
            'startDateTime': f'{start_date}T00:0:00',
            'endDateTime': f'{end_date}T23:59:59',
            'unitGroup': 'metric',
            'location': location,
            'key': self.key,
            'contentType': 'json',
        }
        data = requests.get(url, params).json()
        return [
            {
                'date': row['datetimeStr'][:10],
                'mint': row['mint'],
                'maxt': row['maxt'],
                'location': 'Volgograd,Russia',
                'humidity': row['humidity'],
            }
            for row in data['locations'][location]['values']
        ]


class Sql_bd(object):
    def __init__(self, url, metadata_in, provider_in, table_in):
        self.url_sql = url
        self.provider = provider_in
        self.table = table_in
        self.metadata = metadata_in
        self.engine = create_engine(self.url_sql)
        self.metadata.create_all(self.engine)
        self.c = self.engine.connect()

    def get_data(self, locate, start_time, end_time):
        self.c.execute(self.table.insert(), self.provider.get_data(locate, start_time, end_time))

    def to_print(self):
        for row in self.c.execute(select([self.table])):
            print(row)


metadata = MetaData()
weather = Table(
    'weather',
    metadata,
    Column('date', String),
    Column('mint', Float),
    Column('maxt', Float),
    Column('location', String),
    Column('humidity', Float),
)
provider = WeatherProvider('I3D60I88UB6KPSDAVGK38HNP5')
bd = Sql_bd('sqlite:///weather.sqlite3', metadata, provider, weather)
bd.get_data('Volgograd,Russia', '2020-09-20', '2020-09-29')
bd.to_print()





