import pysftp
import cx_Oracle
import hashlib
import os
import time
from datetime import datetime

# Configuration
SFTP_HOST = 'your_sftp_server'
SFTP_PORT = 22
SFTP_USERNAME = 'your_username'
SFTP_PASSWORD = 'your_password'
REMOTE_DIR = '/path/to/remote/directory'
ORACLE_DSN = 'your_oracle_dsn'
ORACLE_USER = 'your_oracle_user'
ORACLE_PASSWORD = 'your_oracle_password'

def get_md5(file_path):
    """Compute MD5 hash of a file."""
    hash_md5 = hashlib.md5()
    with open(file_path, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            hash_md5.update(chunk)
    return hash_md5.hexdigest()

def log_to_db(file_name, file_md5, timestamp):
    """Insert file information into the Oracle DB."""
    connection = cx_Oracle.connect(ORACLE_USER, ORACLE_PASSWORD, ORACLE_DSN)
    cursor = connection.cursor()
    cursor.execute(
        """
        INSERT INTO file_logs (file_name, file_md5, timestamp)
        VALUES (:1, :2, :3)
        """,
        (file_name, file_md5, timestamp)
    )
    connection.commit()
    cursor.close()
    connection.close()

def parse_file(file_path):
    """Parse the new file as needed. Placeholder for actual parsing logic."""
    with open(file_path, 'r') as file:
        # Example: Print file content
        print(f'Parsing file {file_path}:')
        print(file.read())

def monitor_sftp():
    """Monitor SFTP server for new files."""
    seen_files = set()
    
    with pysftp.Connection(SFTP_HOST, username=SFTP_USERNAME, password=SFTP_PASSWORD, port=SFTP_PORT) as sftp:
        while True:
            # Change to the desired remote directory
            sftp.cwd(REMOTE_DIR)
            # List files in the remote directory
            remote_files = set(sftp.listdir())
            
            # Check for new files
            new_files = remote_files - seen_files
            
            for new_file in new_files:
                print(f'New file detected: {new_file}')
                
                # Download new file to local storage
                local_file_path = os.path.join('/tmp', new_file)  # Use a suitable local directory
                sftp.get(new_file, local_file_path)
                
                # Compute MD5 checksum
                file_md5 = get_md5(local_file_path)
                print(f'MD5 of {new_file}: {file_md5}')
                
                # Get current UTC timestamp
                timestamp = datetime.utcnow()
                
                # Log to DB
                log_to_db(new_file, file_md5, timestamp)
                
                # Parse the new file
                parse_file(local_file_path)
                
                # Clean up local file if necessary
                os.remove(local_file_path)
            
            # Update seen files
            seen_files.update(new_files)
            
            # Wait before checking again
            time.sleep(10)  # Check every 10 seconds

if __name__ == '__main__':
    monitor_sftp()
