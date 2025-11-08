import mysql.connector
from mysql.connector import Error

class ExecuteQuery:
    """A reusable context manager that handles connection and query execution."""
    
    def __init__(self, host, database, user, password, query, params=None, port=3306):
        """
        Initialize the context manager with connection details and query.
        
        Args:
            host: MySQL server host
            database: Database name
            user: MySQL username
            password: MySQL password
            query: SQL query to execute
            params: Query parameters (tuple or list)
            port: MySQL port (default: 3306)
        """
        self.host = host
        self.database = database
        self.user = user
        self.password = password
        self.port = port
        self.query = query
        self.params = params if params is not None else ()
        self.connection = None
        self.cursor = None
        self.results = None
    
    def __enter__(self):
        """
        Open connection, execute query, and return results when entering the context.
        
        Returns:
            results: Query results (list of tuples)
        """
        print(f"Opening connection to MySQL database '{self.database}' at {self.host}")
        
        # Establish connection
        self.connection = mysql.connector.connect(
            host=self.host,
            database=self.database,
            user=self.user,
            password=self.password,
            port=self.port
        )
        
        # Create cursor and execute query
        self.cursor = self.connection.cursor()
        print(f"Executing query: {self.query}")
        print(f"With parameters: {self.params}")
        
        self.cursor.execute(self.query, self.params)
        
        # Fetch results
        self.results = self.cursor.fetchall()
        print(f"Query executed successfully. Retrieved {len(self.results)} rows.")
        
        return self.results
    
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
        if self.cursor:
            self.cursor.close()
        
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
    # MySQL connection parameters
    config = {
        'host': 'localhost',
        'database': 'testdb',
        'user': 'your_username',
        'password': 'your_password',
        'port': 3306
    }
    
    # First, create a sample users table with age column
    print("Setting up sample database...")
    try:
        with mysql.connector.connect(
            host=config['host'],
            database=config['database'],
            user=config['user'],
            password=config['password']
        ) as conn:
            cursor = conn.cursor()
            cursor.execute('DROP TABLE IF EXISTS users')
            cursor.execute('''
                CREATE TABLE users (
                    id INT PRIMARY KEY AUTO_INCREMENT,
                    name VARCHAR(100) NOT NULL,
                    email VARCHAR(100) NOT NULL,
                    age INT NOT NULL
                )
            ''')
            cursor.execute("INSERT INTO users (name, email, age) VALUES ('Alice', 'alice@example.com', 28)")
            cursor.execute("INSERT INTO users (name, email, age) VALUES ('Bob', 'bob@example.com', 22)")
            cursor.execute("INSERT INTO users (name, email, age) VALUES ('Charlie', 'charlie@example.com', 35)")
            cursor.execute("INSERT INTO users (name, email, age) VALUES ('Diana', 'diana@example.com', 30)")
            cursor.execute("INSERT INTO users (name, email, age) VALUES ('Eve', 'eve@example.com', 19)")
            conn.commit()
        print("Sample database created with users and ages\n")
    except Error as e:
        print(f"Error setting up database: {e}\n")
    
    # Use the ExecuteQuery context manager with the specified query
    print("=" * 60)
    print("Using ExecuteQuery context manager:")
    print("=" * 60)
    
    try:
        query = "SELECT * FROM users WHERE age > %s"
        parameter = (25,)  # Note: params must be a tuple
        
        with ExecuteQuery(**config, query=query, params=parameter) as results:
            print("\nQuery Results (Users with age > 25):")
            print(f"{'ID':<5} {'Name':<15} {'Email':<25} {'Age':<5}")
            print("-" * 60)
            for row in results:
                print(f"{row[0]:<5} {row[1]:<15} {row[2]:<25} {row[3]:<5}")
    except Error as e:
        print(f"Error executing query: {e}")
    
    print("\n" + "=" * 60)
    print("Context manager demonstration complete!")
    print("=" * 60)
