import psycopg2

def get_table_column(cursor, table_name):
    cursor.execute(f"SELECT column_name FROM information_schema.columns WHERE table_name = '{table_name}'")
    return [row[0] for row in cursor.fetchall()]

def get_table_schema(cursor, table_name):
    cursor.execute(f"SELECT column_name, data_type FROM information_schema.columns WHERE table_name = '{table_name}'")
    return ", ".join([f"{row[0]} {row[1]}" for row in cursor.fetchall()])

try:
    conn_source = psycopg2.connect(host="localhost", port="5432", database="store", user="postgres", password="pass")
    conn_target = psycopg2.connect(host="localhost", port="15432", database="store", user="postgres", password="pass")

    cursor_source = conn_source.cursor()
    cursor_target = conn_target.cursor()

    table_names = ["brands","orders","products","order_details"]

    for table_name in table_names:
        schema = get_table_schema(cursor_source, table_name)
        cursor_target.execute(f"CREATE TABLE IF NOT EXISTS {table_name} ({schema});")

        columns = ', '.join(get_table_column(cursor_source, table_name)) 
        cursor_source.execute(f"SELECT * FROM {table_name}")
        rows = cursor_source.fetchall()

        for row in rows:
            values = ', '.join(f"'{value}'" for value in row)
            cursor_target.execute(f"INSERT INTO {table_name} ({columns}) VALUES ({values})")
    
    conn_target.commit()
except Exception as e:
    print(f"An error occurred: {e}")

finally:
    conn_source.close()
    conn_target.close()
