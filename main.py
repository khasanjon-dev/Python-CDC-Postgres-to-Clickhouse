import psycopg2
from clickhouse_driver import Client

# PostgreSQL connection details
pg_conn = psycopg2.connect(
    dbname="postgres",
    user="postgres",
    password="1",
    host="localhost",
    port="5432"
)

# ClickHouse connection details
ch_client = Client(
    host='localhost',
    user='default',
    password='default',
    database='default'
)


def get_schemas_and_tables(pg_conn):
    cursor = pg_conn.cursor()
    cursor.execute("""
        SELECT table_schema, table_name
        FROM information_schema.tables
        WHERE table_type = 'BASE TABLE' AND table_schema NOT IN ('information_schema', 'pg_catalog')
    """)
    return cursor.fetchall()


def transfer_table_data(pg_conn, ch_client, schema, table):
    cursor = pg_conn.cursor()
    cursor.execute(f'SELECT * FROM {schema}.{table}')
    columns = [desc[0] for desc in cursor.description]
    rows = cursor.fetchall()

    # Create destination table name with codename_schema
    ch_table = f'{table}'

    # Insert data into ClickHouse
    for row in rows:
        ch_client.execute(
            f'INSERT INTO {ch_table} ({", ".join(columns)}, codename_schema) VALUES',
            [tuple(row) + (schema,)]
        )


schemas_and_tables = get_schemas_and_tables(pg_conn)

for schema, table in schemas_and_tables:
    transfer_table_data(pg_conn, ch_client, schema, table)

pg_conn.close()
