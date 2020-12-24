import requests
from sqlalchemy import create_engine, Table, Column, String, Float, MetaData
from sqlalchemy.sql import select
from abc import ABCMeta, abstractmethod

class BDInterface:
    __metaclass__ = ABCMeta

    @abstractmethod
    def write_data(self, provider_in): raise NotImplementedError
    def to_print(self): raise NotImplementedError

class ProvInterface:
    __metaclass__ = ABCMeta

    @abstractmethod
    def get_data(self, location, start_date, end_date): raise NotImplementedError

class WeatherProvider(ProvInterface):
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
        print(data)
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


class Sql_bd(BDInterface):
    def __init__(self, url, metadata_in,  table_in):
        self.url_sql = url
        self.table = table_in
        self.metadata = metadata_in
        self.engine = create_engine(self.url_sql)
        self.metadata.create_all(self.engine)
        self.c = self.engine.connect()

    def write_data(self, provider_in):
        self.c.execute(self.table.insert(), provider_in)

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
bd = Sql_bd('sqlite:///weather.sqlite3', metadata, weather)
bd.write_data(provider.get_data('Volgograd,Russia', '2020-09-20', '2020-09-29'))
bd.to_print()





