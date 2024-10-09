import paramiko
import sqlite3
import time
import os

# Establishing SQLite database
def create_db():
    conn = sqlite3.connect('sftp_monitor.db')
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS files (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            filename TEXT,
            timestamp TEXT
        )
    ''')
    conn.commit()
    return conn, cursor

# Check if file exists in database
def file_exists_in_db(cursor, filename):
    cursor.execute('SELECT filename FROM files WHERE filename = ?', (filename,))
    return cursor.fetchone() is not None

# Insert new file entry into database
def save_new_file(cursor, filename):
    cursor.execute('INSERT INTO files (filename, timestamp) VALUES (?, CURRENT_TIMESTAMP)', (filename,))
    cursor.connection.commit()

# Parsing the file (dummy parsing function, modify as needed)
def parse_file(sftp, filepath, localpath):
    # Download the file from the SFTP server
    sftp.get(filepath, localpath)

    # Example of parsing - Read the file and print its contents
    with open(localpath, 'r') as f:
        content = f.read()
        print(f"Parsing file {localpath}:")
        print(content)

# Monitor SFTP for new files
def monitor_sftp(sftp, remote_directory, cursor):
    # List files in the remote directory
    current_files = sftp.listdir(remote_directory)
    
    for file in current_files:
        remote_filepath = os.path.join(remote_directory, file)
        
        # Check if file is already in the database
        if not file_exists_in_db(cursor, file):
            print(f"New file detected: {file}")
            
            # Save the new file name in the SQLite database
            save_new_file(cursor, file)
            
            # Local path to save the downloaded file
            local_filepath = os.path.join('downloads', file)
            
            # Parse the new file (you can add additional processing here)
            parse_file(sftp, remote_filepath, local_filepath)

# SFTP connection setup
def sftp_connect(hostname, port, username, password):
    transport = paramiko.Transport((hostname, port))
    transport.connect(username=username, password=password)
    sftp = paramiko.SFTPClient.from_transport(transport)
    return sftp

if __name__ == '__main__':
    # SFTP connection details
    hostname = 'sftp.example.com'
    port = 22
    username = 'your_username'
    password = 'your_password'
    remote_directory = '/path/to/remote/directory'
    
    # Create the downloads folder if it doesn't exist
    os.makedirs('downloads', exist_ok=True)
    
    # Connect to the SQLite database
    conn, cursor = create_db()
    
    try:
        # Establish SFTP connection
        sftp = sftp_connect(hostname, port, username, password)
        
        print("Monitoring SFTP server for new files...")
        
        # Monitor the SFTP directory for new files in a loop
        while True:
            monitor_sftp(sftp, remote_directory, cursor)
            
            # Wait for a while before checking again (e.g., 60 seconds)
            time.sleep(60)
    
    except Exception as e:
        print(f"Error: {e}")
    
    finally:
        # Close the database and SFTP connections
        conn.close()
        sftp.close()
