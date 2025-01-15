import psycopg2
from clickhouse_driver import Client

# PostgreSQL connection
pg_conn = psycopg2.connect(
    dbname="postgres", user="postgres", password="1", host="localhost", port="5432"
)

# ClickHouse connection
ch_client = Client(host='localhost')


def transfer_data():
    with pg_conn.cursor() as cursor:
        # Query all schemas and tables
        cursor.execute("""
            SELECT table_schema, table_name
            FROM information_schema.tables
            WHERE table_type = 'BASE TABLE' AND table_schema NOT IN ('information_schema', 'pg_catalog');
        """)
        tables = cursor.fetchall()

        for schema, table in tables:
            cursor.execute(f'SELECT * FROM "{schema}"."{table}"')
            rows = cursor.fetchall()
            column_names = [desc[0] for desc in cursor.description]

            # Prepare data for ClickHouse
            clickhouse_data = [
                {'codename_schema': schema, **dict(zip(column_names, row))} for row in rows
            ]

            # Insert data into ClickHouse
            ch_client.execute(
                f"INSERT INTO consolidated_{table} (codename_schema, {', '.join(column_names)}) VALUES",
                clickhouse_data,
            )


transfer_data()
