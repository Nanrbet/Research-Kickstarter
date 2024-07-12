import sqlite3
import csv

from working_ import OUTPUT_PATH

# Usage example:
DATABASE_FILE = 'new_projects.db'
TABLE_NAME = 'projects'
OUTPUT_FILENAME = 'output_db.csv'


def convert_sqlite_to_csv(database_file, table_name, csv_file):
    # Connect to the SQLite database
    conn = sqlite3.connect(database_file)
    cursor = conn.cursor()

    # Execute a SELECT query to fetch all rows from the specified table
    cursor.execute(f"SELECT * FROM {table_name}")
    rows = cursor.fetchall()

    # Get the column names from the cursor description
    column_names = [desc[0] for desc in cursor.description]

    # Write the data to the CSV file
    with open(csv_file, 'w', newline='', encoding='utf-8') as file:
        writer = csv.writer(file)
        writer.writerow(column_names)  # Write the column names as the first row
        writer.writerows(rows)  # Write the data rows

    # Close the connections
    cursor.close()
    conn.close()

convert_sqlite_to_csv(DATABASE_FILE, TABLE_NAME, OUTPUT_FILENAME)