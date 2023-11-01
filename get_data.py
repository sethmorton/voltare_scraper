import psycopg2
from psycopg2 import Error
from os.path import join, dirname
from dotenv import load_dotenv
import os
dotenv_path = join(dirname(__file__), '.env')
load_dotenv(dotenv_path)



# Define the PostgreSQL connection parameters
dbHost = os.environ.get("DB_HOST")
dbName = os.environ.get("DB_NAME")
dbUser = os.environ.get("DB_USER")
dbPassword = os.environ.get("DB_PASSWORD")
dbPort = os.environ.get("DB_PORT")


connection = psycopg2.connect(
        host=dbHost,
        database=dbName,
        user=dbUser,
        password=dbPassword,
        port=dbPort
    )
try:
    # Create a connection to the PostgreSQL database

    # Create a cursor object
    cursor = connection.cursor()

    # Define the SQL query to retrieve the data
    select_query = "SELECT starting_hour, raw_date, real_time_price FROM electricity_prices LIMIT 5;"

    # Execute the SQL query
    cursor.execute(select_query)

    # Fetch all the rows from the result of the query
    records = cursor.fetchall()

    # Print each row
    for row in records:
        print(f"Starting Hour: {row[0]}, Raw Date: {row[1]}, Real Time Price: {row[2]}")

except (Exception, Error) as error:
    print("Error while connecting to PostgreSQL", error)

finally:
    # Close the database connection
    if connection:
        cursor.close()
        connection.close()
        print("PostgreSQL connection is closed")