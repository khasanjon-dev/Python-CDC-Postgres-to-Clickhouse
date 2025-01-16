import json
from datetime import datetime

import psycopg2
from clickhouse_driver import Client
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

# PostgreSQL connection parameters
pg_conn_params = {
    'dbname': 'postgres',
    'user': 'postgres',
    'password': '1',
    'host': 'localhost',
    'port': '5432'
}

# ClickHouse connection parameters
ch_conn_params = {
    'host': 'localhost',
    'port': 9000,
    'user': 'default',
    'password': ''
}

# Connect to PostgreSQL
pg_conn = psycopg2.connect(**pg_conn_params)
pg_conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
pg_cursor = pg_conn.cursor()

# Connect to ClickHouse
ch_client = Client(**ch_conn_params)

# Listen for changes in PostgreSQL
pg_cursor.execute("LISTEN user_change;")

print("Waiting for notifications on channel 'user_change'...")

while True:
    # Wait for notifications
    pg_conn.poll()
    while pg_conn.notifies:
        notify = pg_conn.notifies.pop()
        payload = json.loads(notify.payload)
        schema = payload.get('schema')
        action = payload.get('action')
        data = payload.get('data')

        # Format the created_at field to remove fractional seconds
        created_at = datetime.strptime(data['created_at'], '%Y-%m-%dT%H:%M:%S.%f').strftime('%Y-%m-%d %H:%M:%S')

        # Determine is_deleted value based on action and insert data into ClickHouse
        is_deleted = 0 if action in ['INSERT', 'UPDATE'] else 1

        ch_client.execute('''
            INSERT INTO postgres.users (id, username, email, created_at, codename_schema, is_deleted)
            VALUES (%(id)s, %(username)s, %(email)s, %(created_at)s, %(codename_schema)s, %(is_deleted)s)
        ''', {
            'id': data['id'],
            'username': data['username'],
            'email': data['email'],
            'created_at': created_at,
            'codename_schema': schema,
            'is_deleted': is_deleted
        })

        print(f"{action} data into ClickHouse: {data}")

# Close connections
pg_cursor.close()
pg_conn.close()
