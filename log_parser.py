import os
import re
import sqlite3
from datetime import datetime

# Define the log directory and SQLite database file
LOG_DIR = '/Users/Work/pg_log_parser/logs'
DB_FILE = 'postgresql_logs.db'

# Regular expression to parse each log line
LOG_PATTERN = re.compile(
    r'^(?P<timestamp>\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}) UTC:(?P<ip>\d+\.\d+\.\d+\.\d+)\((?P<port>\d+)\):(?P<user>[^@]+)@(?P<db>[^:]+):\[(?P<pid>\d+)\]:LOG:\s+duration:\s+(?P<duration>[\d.]+) ms\s+statement:\s+(?P<statement>.*)$'
)

# Set up the SQLite database and create table if it doesn't exist
def initialize_db():
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS logs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp TEXT,
            ip TEXT,
            port INTEGER,
            user TEXT,
            db TEXT,
            pid INTEGER,
            duration REAL,
            statement TEXT
        )
    ''')
    conn.commit()
    conn.close()

# Parse log line and return data if it matches the pattern
def parse_log_line(line):
    match = LOG_PATTERN.match(line)
    if match:
        data = match.groupdict()
        data['timestamp'] = datetime.strptime(data['timestamp'], '%Y-%m-%d %H:%M:%S')
        data['port'] = int(data['port'])
        data['pid'] = int(data['pid'])
        data['duration'] = float(data['duration'])
        return data
    return None

# Insert parsed data into the SQLite database
def insert_into_db(data):
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.execute('''
        INSERT INTO logs (timestamp, ip, port, user, db, pid, duration, statement)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    ''', (data['timestamp'], data['ip'], data['port'], data['user'], data['db'], data['pid'], data['duration'], data['statement']))
    conn.commit()
    conn.close()

# Process all log files in the directory
def process_log_files():
    for filename in os.listdir(LOG_DIR):
        filepath = os.path.join(LOG_DIR, filename)
        with open(filepath, 'r') as file:
            for line in file:
                data = parse_log_line(line)
                if data:
                    insert_into_db(data)

# Main script execution
if __name__ == "__main__":
    initialize_db()
    process_log_files()
    print("Log files processed and data inserted into SQLite database.")
