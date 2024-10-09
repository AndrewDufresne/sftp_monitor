import pysftp
import hashlib
import logging
import cx_Oracle
import os
from datetime import datetime
import time

# Oracle DB configuration
DB_USER = 'your_db_user'
DB_PASSWORD = 'your_db_password'
DB_DSN = 'your_db_host/your_db_service'

# SFTP configuration
SFTP_HOST = 'your_sftp_host'
SFTP_USERNAME = 'your_sftp_username'
SFTP_PASSWORD = 'your_sftp_password'
SFTP_DIR = '/remote/path/to/monitor'

# Logging configuration
logging.basicConfig(filename='file_monitor.log', level=logging.INFO, format='%(asctime)s - %(message)s')

# Connect to Oracle DB
def get_db_connection():
    try:
        connection = cx_Oracle.connect(DB_USER, DB_PASSWORD, DB_DSN)
        return connection
    except cx_Oracle.DatabaseError as e:
        logging.error(f"Database connection error: {e}")
        return None

# Get list of files from Oracle DB
def get_files_from_db(connection):
    try:
        cursor = connection.cursor()
        cursor.execute("SELECT filename FROM monitored_files")
        files = cursor.fetchall()
        return set(f[0] for f in files)  # Return as a set of filenames
    except cx_Oracle.DatabaseError as e:
        logging.error(f"Error fetching files from DB: {e}")
        return set()

# Insert new file information into Oracle DB
def save_file_to_db(connection, filename, md5_hash):
    try:
        cursor = connection.cursor()
        timestamp = datetime.utcnow()
        cursor.execute(
            "INSERT INTO monitored_files (filename, md5_hash, timestamp) VALUES (:1, :2, :3)",
            [filename, md5_hash, timestamp]
        )
        connection.commit()
    except cx_Oracle.DatabaseError as e:
        logging.error(f"Error saving file to DB: {e}")

# Calculate MD5 hash of a file
def calculate_md5(file_content):
    md5_hash = hashlib.md5()
    md5_hash.update(file_content)
    return md5_hash.hexdigest()

# Parse the new file (dummy function, modify as needed)
def parse_file(file_content):
    logging.info(f"Parsing file content: {file_content[:100]}...")  # Adjust logic as per your needs

# Monitor SFTP server for new files
def monitor_sftp_server():
    with pysftp.Connection(SFTP_HOST, username=SFTP_USERNAME, password=SFTP_PASSWORD) as sftp:
        sftp.cwd(SFTP_DIR)
        with get_db_connection() as db_conn:
            if db_conn is None:
                return

            existing_files = get_files_from_db(db_conn)

            for file_attr in sftp.listdir_attr():
                filename = file_attr.filename

                if filename not in existing_files:
                    # Download the new file
                    with sftp.open(filename) as remote_file:
                        file_content = remote_file.read()

                    # Calculate MD5 and log
                    md5_hash = calculate_md5(file_content)
                    logging.info(f"New file detected: {filename}, MD5: {md5_hash}")

                    # Save file info to DB
                    save_file_to_db(db_conn, filename, md5_hash)

                    # Parse the file
                    parse_file(file_content)

if __name__ == "__main__":
    while True:
        monitor_sftp_server()
        time.sleep(300)  # Check every 5 minutes (300 seconds)
