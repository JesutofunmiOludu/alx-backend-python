import mysql.connector
from mysql.connector import Error

class DatabaseConnection:
    """A context manager for handling MySQL database connections."""
    
    def __init__(self, host, database, user, password, port=3306):
        """
        Initialize the context manager with MySQL connection parameters.
        
        Args:
            host: MySQL server host
            database: Database name
            user: MySQL username
            password: MySQL password
            port: MySQL port (default: 3306)
        """
        self.host = host
        self.database = database
        self.user = user
        self.password = password
        self.port = port
        self.connection = None
        self.cursor = None
    
    def __enter__(self):
        """
        Open database connection when entering the context.
        
        Returns:
            cursor: Database cursor object for executing queries
        """
        print(f"Opening connection to MySQL database '{self.database}' at {self.host}")
        self.connection = mysql.connector.connect(
            host=self.host,
            database=self.database,
            user=self.user,
            password=self.password,
            port=self.port
        )
        self.cursor = self.connection.cursor()
        return self.cursor
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """
        Close database connection when exiting the context.
        
        Args:
            exc_type: Exception type if an error occurred
            exc_val: Exception value if an error occurred
            exc_tb: Exception traceback if an error occurred
        
        Returns:
            False to propagate exceptions
        """
        if self.connection:
            if exc_type is None:
                # No exception occurred, commit changes
                self.connection.commit()
                print("Changes committed successfully")
            else:
                # Exception occurred, rollback changes
                self.connection.rollback()
                print(f"Error occurred: {exc_val}. Rolling back changes")
            
            self.connection.close()
            print("Database connection closed")
        
        return False  # Propagate any exceptions


# Example usage
if __name__ == "__main__":
    # Create a sample database and users table
    print("Setting up sample database...")
    with sqlite3.connect('example.db') as conn:
        cursor = conn.cursor()
        cursor.execute('DROP TABLE IF EXISTS users')
        cursor.execute('''
            CREATE TABLE users (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                email TEXT NOT NULL
            )
        ''')
        cursor.execute("INSERT INTO users VALUES (1, 'Alice', 'alice@example.com')")
        cursor.execute("INSERT INTO users VALUES (2, 'Bob', 'bob@example.com')")
        cursor.execute("INSERT INTO users VALUES (3, 'Charlie', 'charlie@example.com')")
        conn.commit()
    print("Sample database created\n")
    
    # Use the custom context manager to query the database
    print("Using DatabaseConnection context manager:")
    print("-" * 50)
    
    with DatabaseConnection('example.db') as cursor:
        cursor.execute('SELECT * FROM users')
        results = cursor.fetchall()
        
        print("\nQuery Results:")
        print(f"{'ID':<5} {'Name':<15} {'Email':<25}")
        print("-" * 50)
        for row in results:
            print(f"{row[0]:<5} {row[1]:<15} {row[2]:<25}")
    
    print("\n" + "=" * 50)
    print("Context manager demonstration complete!")
