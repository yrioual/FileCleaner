import os
import sys
import glob
import datetime
import hashlib
import platform
import concurrent.futures

def get_directory():
    args = sys.argv[1:]
    if not args:
        print("Please provide a directory path as argument")
        sys.exit(1)
    
    path = args[0]
    if not os.path.isdir(path):
        print(f"Error: '{path}' is not a valid directory")
        sys.exit(1)
        
    return os.path.abspath(path)

def get_creation_time(file_path):
    """
    Get the creation time of a file.

    Args:
        file_path (str): Path to the file

    Returns:
        datetime.datetime: Creation timestamp as a DateTime object
    """
    try:
        # Windows uses getctime, Unix uses getmtime as best approximation
        if platform.system() == 'Windows':
            timestamp = os.path.getctime(file_path)
        else:
            timestamp = os.path.getmtime(file_path)
            
        return datetime.datetime.fromtimestamp(timestamp)
        
    except OSError as e:
        print(f"Error getting creation time for {file_path}: {e}")
        return datetime.datetime.now()

def get_last_modification_time(file_path):
    """
    Get the last modification time of a file.

    Args:
        file_path (str): Path to the file

    Returns:
        datetime.datetime: Last modification timestamp as a DateTime object
    """
    try:
        timestamp = os.path.getmtime(file_path)
        return datetime.datetime.fromtimestamp(timestamp)
    except OSError as e:
        print(f"Error getting modification time for {file_path}: {e}")
        return datetime.datetime.now()

def calculate_file_hash(file_path):
    if not os.path.isfile(file_path):
        return None, None
        
    try:
        with open(file_path, 'rb') as f:
            hasher = hashlib.md5()
            chunk_size = 8192  # 8KB chunks
            
            while chunk := f.read(chunk_size):
                hasher.update(chunk)
                
            return file_path, hasher.hexdigest()
                
    except (IOError, PermissionError) as e:
        print(f"Error reading file: {file_path}")
        print(f"Reason: {str(e)}")
        return None, None

def scan_directory(file_list):
    # check the files on the disk
    # Create a dictionary to store file hashes and their paths
    file_hashes = {}
    duplicates = []
    # Use ProcessPoolExecutor for CPU-bound hash calculations
    with concurrent.futures.ProcessPoolExecutor() as executor:
        # Submit all file hash calculations to the process pool
        future_to_file = {executor.submit(calculate_file_hash, file_path): file_path 
                         for file_path in file_list}
        
        # Process results as they complete
        for future in concurrent.futures.as_completed(future_to_file):
            file_path, file_hash = future.result()
            if file_hash:
                if file_hash in file_hashes:
                    duplicates.append((file_path, file_hashes[file_hash]))
                else:
                    file_hashes[file_hash] = file_path

    # Print duplicate files found
    if duplicates:
        print(f"\nFound {len(duplicates)} duplicate files:")
        for dup_file, orig_file in duplicates:
            # The file with the later modification time is the duplicate
            duplicate_file = dup_file if get_last_modification_time(dup_file) > get_last_modification_time(orig_file) else orig_file
            original_file = orig_file if duplicate_file == dup_file else dup_file
            
            print(f"\nOriginal: {original_file}")
            print(f"Duplicate: {duplicate_file}")
            
            while True:
                try:
                    delete = input("Delete the duplicate? (Y/N): ").strip().upper()
                    if delete in ['Y', 'N']:
                        break
                    print("Please enter Y or N")
                except KeyboardInterrupt:
                    print("\nOperation cancelled by user")
                    sys.exit(0)

            if delete == 'Y':
                try:
                    os.remove(duplicate_file)
                    print(f"Successfully deleted: {duplicate_file}")
                except OSError as e:
                    print(f"Error deleting file {duplicate_file}: {e}")
    else:
        print("\nNo duplicate files found")

def main():
    directory = get_directory()
    dir_files = []
    dir_files = glob.glob(os.path.join(directory, "**/*"), recursive=True)

    scan_directory(dir_files)

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nOperation cancelled by user")
        sys.exit(0)