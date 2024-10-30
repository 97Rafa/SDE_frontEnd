import psycopg2
from psycopg2 import sql

# Define your connection parameters
DB_HOST = '172.18.0.42'        # Change to 'db' if you are using Docker Compose with a linked service
DB_PORT = '5432'             # Default PostgreSQL port
DB_NAME = 'postgres'
DB_USER = 'postgres'
DB_PASSWORD = 'postgres'

# List the tables you want to drop
tables_to_drop = ['datain', 'estimations', 'requests']

def drop_tables():
    try:
        # Connect to the PostgreSQL database
        connection = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD
        )
        connection.autocommit = True  # Ensure each command executes immediately

        # Create a cursor
        cursor = connection.cursor()

        for table in tables_to_drop:
            try:
                # Drop each table using SQL command
                cursor.execute(sql.SQL("DROP TABLE IF EXISTS {} CASCADE").format(sql.Identifier(table)))
                print(f"Table '{table}' dropped successfully.")
            except Exception as e:
                print(f"Error dropping table '{table}': {e}")

        # Close the cursor and connection
        cursor.close()
        connection.close()
        print("Connection closed.")

    except Exception as e:
        print(f"Failed to connect to the database: {e}")

if __name__ == "__main__":
    drop_tables()
