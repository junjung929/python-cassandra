from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
import os
import logging

DB_HOSTS = ['localhost']
DB_PORT = 9042
DB_USERNAME = 'cassandra'
DB_PASSWORD = 'cassandra'
DB_KEYSPACE = 'test'

if os.environ.get('DB_HOSTS'):
    if os.environ.get('DB_PORT'):
        DB_HOSTS = os.environ.get('DB_HOSTS')
        DB_PORT = os.environ.get('DB_PORT')
        # Debugging database connection in cloud
        logging.warn('DB_HOSTS is ' +
                     str(DB_HOSTS) + ':' + str(DB_PORT))
if os.environ.get('DB_USERNAME'):
    if os.environ.get('DB_PASSWORD'):
        DB_USERNAME = os.environ.get('DB_USERNAME')
        DB_PASSWORD = os.environ.get('DB_PASSWORD')
if os.environ.get('DB_KEYSPACE'):
    DB_KEYSPACE = os.environ.get('DB_KEYSPACE')
    logging.warn('Connected keyspace: ' + str(DB_KEYSPACE))

auth_provider = PlainTextAuthProvider(
    username=DB_USERNAME, password=DB_PASSWORD)
cluster = Cluster(DB_HOSTS, port=DB_PORT, auth_provider=auth_provider)
client = cluster.connect(DB_KEYSPACE)
logging.warn('DB_HOSTS are ' +
                     str(DB_HOSTS) + ':' + str(DB_PORT))

print('[Cassandra] Connection to keyspace \''+ DB_KEYSPACE+'\' succeed.')
restaurants = ['RESTAURANT1', 'RESTAURANT2']

for restaurant in restaurants:
    client.execute('CREATE TABLE IF NOT EXISTS ' + restaurant +
                   '_chunks (hour int, minute int, weekday int, \
                   day date, min int, max int, avg int, heatmap list<text>, \
                   img_heatmap blob, PRIMARY KEY (day));')
result = client.execute('select * from user;')
for row in result:
    print(row)

