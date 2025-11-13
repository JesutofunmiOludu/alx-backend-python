from daytime import datetime
import sqlite3
import functools


def log_queries(func):
    def wrapper(*args, **kwargs):
        
        query = args[1]  # Assuming the second argument is always the query
        print(f'Executing query: {query} for args: {args[2:]}, kwargs: {kwargs}')
        return func(*args, **kwargs)
        
        return wrapper

@log_queries
def fetch_all_users(query):
    conn = sqlite3.connect('users.db')
    cursor = conn.cursor()
    cursor.execute(query)
    results = cursor.fetchall()
    conn.close()
    return results


# Fetch users while logging the query
users = fetch_all_users(query="SELECT * FROM users")

print("Fetched users:", users)
