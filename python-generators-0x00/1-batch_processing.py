import mysql.connector
from mysql.connector import Error

def stream_users_in_batches(batch_size):
    """
    Generator function that fetches rows from user_data table
    in batches using yield.
    """
    try:
        # Connect to ALX_prodev database
        connection = mysql.connector.connect(
            host='localhost',
            user='root',
            password='your_mysql_password',
            database='ALX_prodev'
        )

        if connection.is_connected():
            cursor = connection.cursor(dictionary=True)
            cursor.execute("SELECT * FROM user_data")

            # Fetch rows in batches
            while True:
                rows = cursor.fetchmany(batch_size)
                if not rows:
                    break
                yield rows  # Yield one batch (list of rows) at a time

    except Error as e:
        print(f"❌ Error: {e}")

    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()


def batch_processing(batch_size):
    """
    Generator that processes each batch from stream_users_in_batches
    and yields users older than 25.
    """
    for batch in stream_users_in_batches(batch_size):  # Loop 1
        # Filter users older than 25
        filtered_users = [user for user in batch if float(user['age']) > 25]  # Loop 2 (inside list comprehension)
        for user in filtered_users:  # Loop 3
            yield user  # Yield one filtered user at a time
