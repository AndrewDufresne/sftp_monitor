import paramiko
import os
import time
from datetime import datetime

# SFTP server details
host = ''
port = 22
username = ''
password = ''
remote_path = '/home/andrew/'
local_state_file = '/dev_env/sftp/local_state.txt'  # Where we keep track of the file changes

def get_sftp_connection():
    """ Establishes the SFTP connection """
    transport = paramiko.Transport((host, port))
    transport.connect(username=username, password=password)
    sftp = paramiko.SFTPClient.from_transport(transport)
    return sftp

def list_files(sftp):
    """ Lists the files in the remote SFTP directory """
    return sftp.listdir_attr(remote_path)

def load_last_state():
    """ Loads the last known state of files from a local file """
    if os.path.exists(local_state_file):
        with open(local_state_file, 'r') as f:
            return f.read().splitlines()
    return []

def save_current_state(files):
    """ Saves the current state of files to a local file """
    with open(local_state_file, 'w') as f:
        for file in files:
            f.write(file + '\n')

def detect_new_files(current_files, previous_files):
    """ Detects new files by comparing current and previous file lists """
    return list(set(current_files) - set(previous_files))

def process_new_file(file_name):
    """ Function to handle a new file, e.g., download or log it """
    print(f"New file detected: {file_name}")
    # Add file processing logic here (e.g., download, parse, etc.)

def monitor_sftp():
    sftp = get_sftp_connection()
    current_files = [file.filename for file in list_files(sftp)]
    previous_files = load_last_state()

    new_files = detect_new_files(current_files, previous_files)
    if new_files:
        for new_file in new_files:
            process_new_file(new_file)

    save_current_state(current_files)
    sftp.close()

if __name__ == "__main__":
    while True:
        monitor_sftp()
        time.sleep(1)  # Sleep for 5 minutes between checks
