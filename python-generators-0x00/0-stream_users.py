import mysql.connector
from mysql.connector import Error

def stream_users():
    """
    Generator function that fetches rows one by one
    from the user_data table using yield.
    """
    try:
        # Connect to the ALX_prodev database
        connection = mysql.connector.connect(
            host='localhost',
            user='root',
            password='psswrd123',
            database='ALX_prodev'
        )

        if connection.is_connected():
            cursor = connection.cursor(dictionary=True)
            cursor.execute("SELECT * FROM user_data")

            # Only ONE loop allowed
            for row in cursor:
                yield row  # Yield one row at a time

    except Error as e:
        print(f"❌ Error: {e}")

    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()

