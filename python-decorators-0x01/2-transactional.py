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

def transactional(func):
    def wrapper(*args, **kwargs):
        result = func(conn, *args, **kwargs)

        conn.commit()
        print(f"Transaction committed for '{func.__name__}'.")
        return result

        if squilite3.Error:
            conn.rollback()
            print(f"[ERROR] Transaction rolled back for '{func.__name__}': {e}")
    return wrapper

@with_db_connection 
@transactional 
def update_user_email(conn, user_id, new_email): 
    cursor = conn.cursor() 
    cursor.execute("UPDATE users SET email = ? WHERE id = ?", (new_email, user_id)) 
#### Update user's email with automatic transaction handling 

update_user_email(user_id=1, new_email='Crawford_Cartwright@hotmail.com')
