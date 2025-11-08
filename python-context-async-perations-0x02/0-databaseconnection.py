import mysql.connector
from mysql.connector import Error

class DatabaseConnection:
    def __init__(self, host, database, user, password):
        self.host = host
        self.database = database
        self.user = user
        self.password = password
        self.connection = None

    def __enter__(self):
        try:
            self.connection = mysql.connector.connect(
                host=self.host,
                database=self.database,
                user=self.user,
                password=self.password
            )
            if self.connection.is_connected():
                print("Connected to the database.")
        except Error as e:
            print(f"Error while connecting to MySQL: {e}")

    def __exit__ (self):
        if self.connection and self.connection.is_connected():
            self.connection.close()
            print("Database connection closed.")



# Example usage:
if __name__ == "__main__":
    # MySQL connection parameters
    config = {
        'host': 'localhost',
        'database': 'testdb',
        'user': 'root',
        'password': 'psswrd123',
        
    }

    try:
        with mysql.connector.connect(**config) as connect:
            cursor = connect.cursor()
            cursor.execute("create database if not exists testdb")
            cursor.execute("""create table if not exists user" 
            userId INT AUTO_INCREMENT PRIMARY KEY, 
            name VARCHAR(255) NOT NULL, 
            email VARCHAR(100) NOT NULL)""")
            cursor.execute("insert into user(name, email) values ('John Doe', 'johndoe@gmail.com')")
            cursor.execute("insert into user(name, email) values ('jack Lewance','jacklee@gmail.com '")
            cursor.execute("insert into user(name, email) values ('Janet Duke','janetduke@gmail.com '")

            connect.commit()
        print('Sample database and table created with sample data.')
    except Error as e:
        print(f"Error setting up database: {e}")

# Use the custom context manager to query the database
print("Using DatabaseConnection context manager:")
print("-" * 50)
try:
   with DatabaseConnection(**config) as cursor:
      cursor.execute("SELECT * FROM user")
      results = cursor.fetchall()
      print("\nQuery Results:")
      print(f"{'ID':<5} {'Name':<15} {'Email':<25}")
      print("-" * 50)
      for row in results:
            print(f"{row[0]:<5} {row[1]:<15} {row[2]:<25}")
except Error as e:
    print(f"Error executing query: {e}")
    
    print("\n" + "=" * 50)
    print("Context manager demonstration complete!")
