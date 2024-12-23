##Data ingestion script


import psycopg2
from datetime import datetime, timedelta

# Connect to Redshift
conn = psycopg2.connect(
    dbname="dev", 
    user="{user}", 
    password="{password}", 
    host="{host_name}", 
    port="5439"
)

cur = conn.cursor()

# Insert 200 rows of data
for i in range(1, 201):
    if 150 <= i <= 160:  # Skewed data
        user_id = i
        first_name = f"User{i}"
        last_name = f"LastName{i}"
        email = f"user{i}@example.com"
        created_at = datetime.now()
        last_login = datetime.now()
    elif i % 10 == 0:  # NULL values
        user_id = i
        first_name = f"User{i}"
        last_name = f"LastName{i}"
        email = f"user{i}@example.com"
        created_at = None
        last_login = None
    else:  # Randomized data
        user_id = i
        first_name = f"User{i}"
        last_name = f"LastName{i}"
        email = f"user{i}@example.com"
        created_at = datetime.now() - timedelta(days=(i % 5))
        last_login = datetime.now() - timedelta(hours=(i % 5))
    # Execute the INSERT query
    query = """
        INSERT INTO users (user_id, first_name, last_name, email, created_at, last_login)
        VALUES (%s, %s, %s, %s, %s, %s)
    """
    cur.execute(query, (user_id, first_name, last_name, email, created_at, last_login))
# Commit the transaction
conn.commit()

# Close the cursor and connection
cur.close()
conn.close()

print("Data insertion completed.")
