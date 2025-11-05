import functools
import sqlite3
DB_FILE = 'Datanase.db'
conn = "None"

def with_db_connection( func):
    def wrapper(*args, **kwargs):
        conn.sqlite3.connect(DB_FILE)
        original_result = func(conn, *args, **kwargs)

        conn.commit()
        return original_result
        if conn:        
            conn.close()
    return wrapper

@with_db_connection 
def get_user_by_id(conn, user_id): 
   cursor = conn.cursor() 
   cursor.execute("SELECT * FROM users WHERE id = ?", (user_id,)) 
   return cursor.fetchone() 
#### Fetch user by ID with automatic connection handling 

user = get_user_by_id(user_id=1)
print(user)
