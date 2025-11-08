import asyncio
import aiosqlite
import time

async def async_fetch_users():
    """
    Asynchronously fetch all users from the database.
    
    Returns:
        list: All users from the users table
    """
    print("Starting async_fetch_users()...")
    start_time = time.time()
    
    async with aiosqlite.connect('example.db') as db:
        async with db.execute('SELECT * FROM users') as cursor:
            results = await cursor.fetchall()
    
    elapsed = time.time() - start_time
    print(f"async_fetch_users() completed in {elapsed:.2f} seconds")
    print(f"Retrieved {len(results)} users")
    
    return results


async def async_fetch_older_users():
    """
    Asynchronously fetch users older than 40 from the database.
    
    Returns:
        list: Users with age > 40
    """
    print("Starting async_fetch_older_users()...")
    start_time = time.time()
    
    async with aiosqlite.connect('example.db') as db:
        async with db.execute('SELECT * FROM users WHERE age > ?', (40,)) as cursor:
            results = await cursor.fetchall()
    
    elapsed = time.time() - start_time
    print(f"async_fetch_older_users() completed in {elapsed:.2f} seconds")
    print(f"Retrieved {len(results)} users older than 40")
    
    return results


async def fetch_concurrently():
    """
    Execute both query functions concurrently using asyncio.gather().
    
    Returns:
        tuple: Results from both queries (all_users, older_users)
    """
    print("=" * 60)
    print("Executing queries concurrently using asyncio.gather()...")
    print("=" * 60)
    
    start_time = time.time()
    
    # Run both queries concurrently
    all_users, older_users = await asyncio.gather(
        async_fetch_users(),
        async_fetch_older_users()
    )
    
    total_elapsed = time.time() - start_time
    print(f"\nTotal execution time: {total_elapsed:.2f} seconds")
    print("=" * 60)
    
    return all_users, older_users


async def setup_database():
    """Create a sample database with users table."""
    print("Setting up sample database...")
    
    async with aiosqlite.connect('example.db') as db:
        # Drop existing table if it exists
        await db.execute('DROP TABLE IF EXISTS users')
        
        # Create users table
        await db.execute('''
            CREATE TABLE users (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                email TEXT NOT NULL,
                age INTEGER NOT NULL
            )
        ''')
        
        # Insert sample data
        users_data = [
            ('Alice', 'alice@example.com', 28),
            ('Bob', 'bob@example.com', 35),
            ('Charlie', 'charlie@example.com', 45),
            ('Diana', 'diana@example.com', 50),
            ('Eve', 'eve@example.com', 32),
            ('Frank', 'frank@example.com', 42),
            ('Grace', 'grace@example.com', 29),
            ('Henry', 'henry@example.com', 55),
        ]
        
        await db.executemany(
            'INSERT INTO users (name, email, age) VALUES (?, ?, ?)',
            users_data
        )
        
        await db.commit()
    
    print("Sample database created with 8 users\n")


def display_results(all_users, older_users):
    """Display the query results in a formatted way."""
    print("\n" + "=" * 60)
    print("RESULTS:")
    print("=" * 60)
    
    # Display all users
    print("\n1. All Users:")
    print(f"{'ID':<5} {'Name':<15} {'Email':<25} {'Age':<5}")
    print("-" * 60)
    for row in all_users:
        print(f"{row[0]:<5} {row[1]:<15} {row[2]:<25} {row[3]:<5}")
    
    # Display older users
    print("\n2. Users Older Than 40:")
    print(f"{'ID':<5} {'Name':<15} {'Email':<25} {'Age':<5}")
    print("-" * 60)
    for row in older_users:
        print(f"{row[0]:<5} {row[1]:<15} {row[2]:<25} {row[3]:<5}")
    
    print("\n" + "=" * 60)


async def main():
    """Main function to orchestrate the async operations."""
    # Setup the database
    await setup_database()
    
    # Fetch data concurrently
    all_users, older_users = await fetch_concurrently()
    
    # Display results
    display_results(all_users, older_users)
    
    print("Async operations completed successfully!")


if __name__ == "__main__":
    # Run the async main function
    asyncio.run(main())
