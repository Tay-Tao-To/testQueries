import psycopg2
from concurrent.futures import ThreadPoolExecutor
import time

# Define the database connection parameters
db_params = {
    "dbname": "your_database",
    "user": "your_username",
    "password": "your_password",
    "host": "localhost",
    "port": 5432
}

# Read the SQL commands from the file
with open('queries.txt', 'r') as file:
    sql_commands = file.read().split(';')

def execute_commands():
    # Connect to the database
    conn = psycopg2.connect(**db_params)
    cur = conn.cursor()

    # Execute each SQL command
    for command in sql_commands:
        if command.strip():  # Ignore empty commands
            cur.execute(command)

    # Close the database connection
    cur.close()
    conn.close()

# Define the number of threads and the number of times to execute the commands
num_threads = 10
num_executions = 1000

# Create a pool of threads
with ThreadPoolExecutor(max_workers=num_threads) as executor:
    # Start time
    start = time.time()
    # Execute the commands the specified number of times
    for _ in range(num_executions):
        executor.submit(execute_commands)
    # End time
    end = time.time()

# Print the time taken
print(f"Time taken: {end - start} seconds")