import mysql.connector
from mysql.connector import Error

def stream_user_ages():
    """
    Generator that yields user ages one by one from the user_data table.
    """
    try:
        connection = mysql.connector.connect(
            host='localhost',
            user='root',
            password='your_mysql_password',
            database='ALX_prodev'
        )

        if connection.is_connected():
            cursor = connection.cursor()
            cursor.execute("SELECT age FROM user_data")

            # Only ONE loop here
            for (age,) in cursor:
                yield float(age)  # yield one age at a time

    except Error as e:
        print(f"❌ Error: {e}")

    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()


def calculate_average_age():
    """
    Calculates the average age of all users using the generator,
    without loading all data into memory.
    Uses at most two loops in total.
    """
    total_age = 0
    count = 0

    # ONE loop here — totals all ages from the generator
    for age in stream_user_ages():
        total_age += age
        count += 1

    if count > 0:
        average = total_age / count
        print(f"Average age of users: {average:.2f}")
    else:
        print("No users found in the database.")
        

if __name__ == "__main__":
    calculate_average_age()
