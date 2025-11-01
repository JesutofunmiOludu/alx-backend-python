import mysql.connector
from mysql.connector import Error

def paginate_users(page_size, offset):
    """
    Fetch a single page of users from the user_data table.
    Returns a list of rows.
    """
    try:
        connection = mysql.connector.connect(
            host='localhost',
            user='root',
            password='your_mysql_password',
            database='ALX_prodev'
        )

        if connection.is_connected():
            cursor = connection.cursor(dictionary=True)
            query = "SELECT * FROM user_data LIMIT %s OFFSET %s"
            cursor.execute(query, (page_size, offset))
            rows = cursor.fetchall()
            return rows

    except Error as e:
        print(f"❌ Error: {e}")
        return []

    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()


def lazy_paginate(page_size):
    """
    Generator that lazily fetches pages of users from the user_data table.
    Only one loop is used.
    """
    offset = 0
    while True:  # <-- Only one loop in the entire function
        page = paginate_users(page_size, offset)
        if not page:
            break  # Stop when no more data
        yield page  # Yield one page at a time
        offset += page_size  # Move to the next page
