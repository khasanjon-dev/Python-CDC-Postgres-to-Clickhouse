import json
import time

import psycopg2
from clickhouse_driver import Client
from psycopg2.extras import LogicalReplicationConnection

# Connect to PostgreSQL using LogicalReplicationConnection
pg_conn = psycopg2.connect(
    dbname='postgres',
    user='postgres',
    password='1',
    host='localhost',
    port='5432',
    connection_factory=LogicalReplicationConnection
)

# Connect to ClickHouse
ch_conn = Client(
    host='localhost',
    user='default',
    password='default',
    database='default'
)


# Function to handle changes from PostgreSQL
def handle_changes():
    # Create a replication cursor
    pg_cur = pg_conn.cursor()

    # Recreate the replication slot if necessary
    try:
        pg_cur.drop_replication_slot('slot_name')
    except psycopg2.errors.UndefinedObject:
        pass  # Slot does not exist, no need to drop

    pg_cur.create_replication_slot('slot_name', output_plugin='pgoutput')
    pg_cur.start_replication(slot_name='slot_name', decode=True,
                             options={'proto_version': '1', 'publication_names': 'all_tables_pub'})

    def consume(msg):
        try:
            change = msg.payload
            for change in change['change']:
                schema = change['schema']
                table = change['table']
                operation = change['kind']
                data = dict(zip(change['columnnames'], change['columnvalues']))

                # Determine target ClickHouse table
                target_table = f'{schema}_{table}'

                # Write data to ClickHouse
                if operation == 'insert':
                    columns = ', '.join(data.keys())
                    values = ', '.join([f"'{v}'" for v in data.values()])
                    ch_conn.execute(f"INSERT INTO {target_table} ({columns}) VALUES ({values})")
                elif operation == 'update':
                    set_clause = ', '.join([f"{k} = '{v}'" for k, v in data.items() if k != 'id'])
                    ch_conn.execute(f"ALTER TABLE {target_table} UPDATE {set_clause} WHERE id = '{data['id']}'")
                elif operation == 'delete':
                    ch_conn.execute(f"ALTER TABLE {target_table} DELETE WHERE id = '{data['id']}'")

            msg.cursor.send_feedback(flush_lsn=msg.data_start)
        except UnicodeDecodeError as e:
            print(f"Decoding error occurred: {e}")

    # Start consuming replication changes
    pg_cur.consume_stream(consume)


# Monitor for schema changes and update ClickHouse schema
def monitor_schema_changes():
    pg_conn_poll = psycopg2.connect(
        dbname='postgres',
        user='postgres',
        password='1',
        host='localhost',
        port='5432'
    )
    pg_cur_poll = pg_conn_poll.cursor()
    pg_cur_poll.execute("LISTEN schema_change;")

    while True:
        pg_conn_poll.poll()
        while pg_conn_poll.notifies:
            notify = pg_conn_poll.notifies.pop(0)
            schema_change = json.loads(notify.payload)
            event_type = schema_change['event_type']
            schema = schema_change['schema']
            table = schema_change['table']

            # Handle the schema change
            if event_type == 'CREATE TABLE':
                create_clickhouse_table(schema, table)
        time.sleep(1)  # Poll every second


def create_clickhouse_table(schema, table):
    # Example: create a new table in ClickHouse if it doesn't exist
    ch_conn.execute(f"""
        CREATE TABLE IF NOT EXISTS {schema}_{table} (
            id UInt32,
            -- Add other column definitions here based on PostgreSQL table schema
        ) ENGINE = MergeTree()
        ORDER BY id
    """)


# Run the schema change monitor in a separate thread
import threading

schema_change_thread = threading.Thread(target=monitor_schema_changes)
schema_change_thread.start()

# Run the handler
handle_changes()
