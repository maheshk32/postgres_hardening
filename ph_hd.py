import psycopg2
import pandas as pd
from tabulate import tabulate

def extract_pg_params(conn):
    cur = conn.cursor()
    cur.execute("select name,setting,unit,context from pg_settings")
    rows = cur.fetchall()
    df = pd.DataFrame(rows, columns=['name', 'setting', 'unit', 'context'])
    return df

def compare_with_file(pg_df, file_path):
    file_df = pd.read_csv(file_path)
    # Verify the columns in the file_df
    print("Columns in the CSV file:", file_df.columns)
    if 'name' not in file_df.columns:
        raise KeyError("The CSV does not contain a 'name' column")
    merged_df = pg_df.merge(file_df, on='name', how='inner', suffixes=('_db', '_file'))
    mismatched_rows = merged_df[merged_df['setting_db'] != merged_df['setting_file']]
    return mismatched_rows

# Get Hostname
def get_hostname(conn):
    dsn_parameters = conn.get_dsn_parameters()
    hostname = dsn_parameters.get('host')
    return hostname

# Connect to the database
conn = psycopg2.connect(
    database="db1",
    user="pg1",
    password="pg1",
    host="postgres1",
    port="5432"
)

# Connect to the control database
conn_ctrl = psycopg2.connect(
    database="control",
    user="pg1",
    password="pg1",
    host="postgres1",
    port="5432"
)

# Extract PostgreSQL parameters
pg_df = extract_pg_params(conn)

# Specify the file path
file_path = "_db_parameters_baseline.txt"

source_hostname = get_hostname(conn)

# Compare and print mismatched parameters
try:
    mismatched_params = compare_with_file(pg_df, file_path)
    if not mismatched_params.empty:
        print(tabulate(mismatched_params, headers='keys', tablefmt='pretty'))
    else:
        print("No mismatched parameters found.")
except KeyError as e:
    print(f"KeyError: {e}")

with conn_ctrl.cursor() as new_cursor:
    # Adjust unpacking to match actual column count
    mismatched_params = compare_with_file(pg_df, file_path)
    for _, row in mismatched_params.iterrows():
        # Always use placeholders now to include source_hostname
        new_cursor.execute(
            "INSERT INTO mismatched_parameters (hostname, name, setting_db, unit_db, context_db, setting_file, unit_file, context_file) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)",
            (source_hostname, row['name'], row['setting_db'], row['unit_db'], row['context_db'], row.get('setting_file', None), row.get('unit_file', None), row.get('context_file', None)))
    # Commit the changes
    conn_ctrl.commit()

# Close the database connection
conn.close()
