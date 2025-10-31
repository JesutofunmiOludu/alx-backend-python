import mysql.connector
import csv
import uuid


# ----------------------------
# 1. Connect to MySQL Server
# ----------------------------

def connect_db( "user" : "root", "password": "","host" : "localhost" ):
   try:
        connection = mysql.connector.connect(
            host=host,
            user=user,
            password=password
        )
        if connection.is_connected():
            print("✅ Connected to MySQL Server")
        return connection
    except Error as e:
        print(f"❌ Error: {e}")
        return None

# ----------------------------
# 2. Create Database
# ----------------------------
def create_database(connection):
  
  try:
        cursor = connection.cursor()
        cursor.execute("CREATE DATABASE IF NOT EXISTS ALX_prodev;")
        print("✅ Database 'ALX_prodev' ensured.")
    except Error as e:
        print(f"❌ Error creating database: {e}")

# ----------------------------
# 3. Connect to ALX_prodev DB
# ----------------------------
def connect_to_prodev(host='localhost', user='root', password=''):
  try:
        connection = mysql.connector.connect(
            host=host,
            user=user,
            password=password,
            database='ALX_prodev'
        )
        if connection.is_connected():
            print("✅ Connected to ALX_prodev database")
        return connection
    except Error as e:
        print(f"❌ Error connecting to ALX_prodev: {e}")
        return None
      
# ----------------------------
# 4. Create user_data Table
# ----------------------------

def create_table(connection):
    create_table_query = """
    CREATE TABLE IF NOT EXISTS user_data (
        user_id CHAR(36) PRIMARY KEY,
        name VARCHAR(255) NOT NULL,
        email VARCHAR(255) NOT NULL,
        age DECIMAL(5,2) NOT NULL,
        INDEX (user_id)
    );
    """
    try:
        cursor = connection.cursor()
        cursor.execute(create_table_query)
        connection.commit()
        print("✅ Table 'user_data' ensured.")
    except Error as e:
        print(f"❌ Error creating table: {e}")

 # ----------------------------
# 5. Insert Data
# ---------------------------- 

def insert_data(connection, data):
  insert_query = """
    INSERT INTO user_data (user_id, name, email, age)
    VALUES (%s, %s, %s, %s);
    """
   try:
        cursor = connection.cursor()
        # Check if email already exists before inserting
        cursor.execute("SELECT email FROM user_data WHERE email = %s", (data['email'],))
        result = cursor.fetchone()
        if result:
            print(f"⚠️ Skipped duplicate email: {data['email']}")
            return

        cursor.execute(insert_query, (data['user_id'], data['name'], data['email'], data['age']))
        connection.commit()
        print(f"✅ Inserted: {data['name']} ({data['email']})")
  except Error as e:
        print(f"❌ Error inserting data: {e}")

# ----------------------------
# 6. Load CSV and Seed Data
# ----------------------------
def seed_data_from_csv(connection, csv_file='user_data.csv'):
  try:
        with open(csv_file, mode='r', encoding='utf-8') as file:
            reader = csv.DictReader(file)
            for row in reader:
                data = {
                    'user_id': str(uuid.uuid4()),
                    'name': row['name'],
                    'email': row['email'],
                    'age': float(row['age'])
                }
                insert_data(connection, data)
        print("🎉 Data seeding completed successfully.")
    except FileNotFoundError:
        print("❌ CSV file not found.")
    except Exception as e:
        print(f"❌ Error reading CSV: {e}")


# ----------------------------
# 7. Main Execution Flow
# ----------------------------
if __name__ == "__main__":
    # Step 1: Connect to MySQL Server
    server_conn = connect_db(user='root', password='your_mysql_password')

    # Step 2: Create Database
    if server_conn:
        create_database(server_conn)
        server_conn.close()

    # Step 3: Connect to ALX_prodev
    db_conn = connect_to_prodev(user='root', password='your_mysql_password')

    # Step 4: Create Table
    if db_conn:
        create_table(db_conn)

        # Step 5: Seed Data
        seed_data_from_csv(db_conn, csv_file='user_data.csv')

        db_conn.close()
  
