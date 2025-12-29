#  This script generates fake order data and inserts it into an SQLite database named 'retail_public.db'.
#  It creates a table named 'Orders' if it doesn't already exist, generates 100 fake orders, and inserts them into the table.
#  The script also includes error handling for database operations and prints out the generated orders for verification
from faker import Faker
import random
import sqlite3

# Initialize Faker
fake = Faker()

# Example customer IDs available in your database
customer_ids = [1, 2]  # Replace with actual customer IDs from your Customers table

def generate_orders(num_orders):
    """Function to generate fake order data."""
    orders_data = []
    for _ in range(num_orders):
        customer_id = random.choice(customer_ids)
        order_date = fake.date_between(start_date='-2y', end_date='today').strftime('%Y-%m-%d')
        total_amount = round(random.uniform(20.0, 5000.0), 2)  # Random amount between $20 and $5000
        order = (customer_id, order_date, total_amount)
        orders_data.append(order)
    return orders_data

try:
    # Connect to SQLite database
    print("Connecting to the database...")
    connection = sqlite3.connect('retail_public.db')  # Ensure this path is correct
    cursor = connection.cursor()
    
    # Create table if it doesn't exist
    print("Checking/creating the Orders table...")
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS Orders (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        customer_id INTEGER,
        order_date TEXT,
        total_amount REAL
    );
    ''')
    connection.commit()  # Commit after creating the table
    
    # Generate sample orders
    print("Generating sample orders...")
    sample_orders = generate_orders(100)
    
    # Insert data using parameterized queries
    print("Inserting orders into the database...")
    for order in sample_orders:
        try:
            cursor.execute(
                'INSERT INTO Orders (customer_id, order_date, total_amount) VALUES (?, ?, ?)', order
            )
        except sqlite3.Error as e:
            print(f"Failed to insert order {order}: {e}")

    # Commit the transaction
    try:
        connection.commit()
        print("Records committed successfully.")
    except sqlite3.Error as e:
        print(f"Commit failed: {e}")
        connection.rollback()  # Rollback in case of any error
    
    # Verify that the records have been inserted
    print("Verifying records in the database...")
    cursor.execute('SELECT * FROM Orders')
    rows = cursor.fetchall()

    if rows:
        for row in rows:
            print(row)
    else:
        print("No records found.")

except sqlite3.Error as e:
    print(f"Database error: {e}")

finally:
    # Close the connection
    if connection:
        connection.close()
        print("Database connection closed.")
