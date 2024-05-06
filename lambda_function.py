import boto3
import os
import psycopg2
from datetime import datetime, timedelta


# Connect to your PostgreSQL database
def connect_to_db():
    conn = psycopg2.connect(
        host=os.getenv('DBHOST'),
        database=os.getenv('DBNAME'),
        user=os.getenv('DBUSER'),
        password=os.getenv('DBPWD')
    )
    return conn


# Update records in PostgreSQL table
def update_records(conn, connection_lost_min):
    try:
        # Open a cursor to perform database operations
        cursor = conn.cursor()

        # Calculate the timestamp
        connection_bias = datetime.utcnow() - timedelta(minutes=connection_lost_min)

        # SQL query to update records
        update_query = """
            UPDATE experiment.participants
            SET empatica_status = false
            WHERE empatica_last_update <= %s
        """
        print(f"db update query {update_query} , value {connection_bias}")
        # Execute the update query
        cursor.execute(update_query, (connection_bias,))
        conn.commit()

        # Close communication with the database
        cursor.close()
    except Exception as e:
        print(f"Error updating records: {e}")
        conn.rollback()
    finally:
        conn.close()


# Lambda handler function
def lambda_handler(event, context):
    print("db-status-lambda started")
    # Connect to PostgreSQL database
    conn = connect_to_db()
    print("db connected")
    # Update records in the table
    connection_lost_min = int(os.getenv('CONNECTION_LOST_MIN'))
    update_records(conn, connection_lost_min)

    print("db-status-lambda completed")


