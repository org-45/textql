# db generator from csv
import sqlite3
import csv
import os

#output db file
DB_NAME = "textql.db"

#csvs dir
DATA_DIR = "data"


# creates table in db
def create_table(conn, table_name, column_defs):
    """Creates a table in the database."""
    columns_str = ", ".join(column_defs)
    sql = f"CREATE TABLE IF NOT EXISTS {table_name} ({columns_str})"
    conn.execute(sql)
    conn.commit()

# handles csv file to execute import on db
def import_csv_to_db(conn, table_name, csv_file):
    """Imports data from a CSV file into the database table."""
    cursor = conn.cursor()
    with open(csv_file, 'r', encoding='utf-8') as file:
        reader = csv.reader(file)
        header = next(reader)
        placeholders = ", ".join("?" * len(header))
        sql = f"INSERT INTO {table_name} VALUES ({placeholders})"
        try:
            cursor.executemany(sql, reader)
            conn.commit()
            print(f"Successfully imported data into {table_name} from {csv_file}")
        except sqlite3.Error as e:
            print(f"Error importing data into {table_name} from {csv_file}: {e}")

# handles creation of index for a given table
def create_index(conn, table_name, column_name):
    """Creates an index on the specified column."""
    index_name = f"idx_{table_name}_{column_name}"
    sql = f"CREATE INDEX IF NOT EXISTS {index_name} ON {table_name} (\"{column_name}\")"
    try:
        conn.execute(sql)
        conn.commit()
        print(f"Index created on table '{table_name}' column '{column_name}'")
    except sqlite3.Error as e:
        print(f"Error creating index on table '{table_name}' column '{column_name}': {e}")

# call this function to import csv to db
def initialize_database_import():
    """Initializes the database: creates tables, imports data, and creates indices."""
    conn = sqlite3.connect(DB_NAME)
    try:
        csv_files = [f for f in os.listdir(DATA_DIR) if f.endswith('.csv')]
        for csv_file in csv_files:
 
            table_name = os.path.splitext(csv_file)[0]
 
            csv_path = os.path.join(DATA_DIR, csv_file)

            with open(csv_path, 'r', encoding='utf-8') as file:
                reader = csv.reader(file)
                header = next(reader)
                column_defs = [f"\"{col}\" TEXT" for col in header]

            create_table(conn, table_name, column_defs)
            import_csv_to_db(conn, table_name, csv_path)

            # create indices
            if table_name == "flights":
                create_index(conn, table_name, "ORIGIN_AIRPORT")
                create_index(conn, table_name, "DESTINATION_AIRPORT")
                create_index(conn, table_name, "AIRLINE")
                create_index(conn, table_name, "FLIGHT_NUMBER")
                create_index(conn, table_name, "TAIL_NUMBER")
                create_index(conn, table_name, "YEAR")
                create_index(conn, table_name, "MONTH")

            elif table_name == "airports":
                create_index(conn, table_name, "IATA_CODE")
                create_index(conn, table_name, "CITY")
                create_index(conn, table_name, "STATE")
                create_index(conn, table_name, "COUNTRY")

            elif table_name == "airlines":
                create_index(conn, table_name, "IATA_CODE")

            print(f"Initialized table {table_name}")

    except Exception as e:
        print(f"Error initializing database: {e}")
    finally:
        conn.close()


if __name__ == "__main__":
    initialize_database_import()
    print("Database initialization complete.")