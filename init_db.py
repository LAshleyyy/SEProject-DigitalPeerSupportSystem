import mysql.connector
import os
from dotenv import load_dotenv

load_dotenv()

db_config = {
    'host': os.getenv('DB_HOST'),
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD'),
    'database': os.getenv('DB_NAME'),
    'port': int(os.getenv('DB_PORT')),
    'ssl_disabled': False
}

def run_schema():
    try:
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor()

        print("Connected to database.")

        with open("db_schema.sql", "r", encoding="utf-8") as file:
            sql_script = file.read()

        # Split safely by semicolon but ignore empty statements
        for statement in sql_script.split(';'):
            statement = statement.strip()
            if statement:
                try:
                    cursor.execute(statement)
                except mysql.connector.Error as err:
                    print("Error executing:", statement)
                    print("Error message:", err)

        conn.commit()
        print("Schema + data inserted successfully.")

    except mysql.connector.Error as err:
        print("Database error:", err)

    finally:
        if conn.is_connected():
            cursor.close()
            conn.close()
            print("Connection closed.")

if __name__ == "__main__":
    run_schema()
