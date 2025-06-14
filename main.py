import os
import hashlib
import shutil
import datetime
import multiprocessing
from tqdm import tqdm

def find_duplicates_multiprocess(directory):
    hashes = {}
    duplicates = []
    files = []
    for root, _, filenames in os.walk(directory):
        files.extend([os.path.join(root, f) for f in filenames])

    with multiprocessing.Pool() as pool, tqdm(total=len(files), desc="Hashing files") as pbar:
        for filepath, file_hash in pool.imap_unordered(hash_file_wrapper, files):
            if file_hash:
                if file_hash in hashes:
                    duplicates.append(filepath)
                else:
                    hashes[file_hash] = filepath
            pbar.update(1)
    return duplicates

def hash_file_wrapper(filepath):
    try:
        if os.path.isfile(filepath):
            return filepath, hash_file(filepath)
        else:
            return filepath, None
    except (IOError, OSError):
        return filepath, None

def hash_file(filepath):
    hasher = hashlib.sha256()
    with open(filepath, 'rb') as f:
        while True:
            chunk = f.read(8192)
            if not chunk:
                break
            hasher.update(chunk)
    return hasher.hexdigest()

def move_duplicates(duplicates, output_dir):
    os.makedirs(output_dir, exist_ok=True)
    moved_files = set()
    with tqdm(total=len(duplicates), desc="Moving duplicates") as pbar:
        for file in duplicates:
            if file not in moved_files:
                dest = os.path.join(output_dir, os.path.basename(file))
                try:
                    shutil.move(file, dest)
                    moved_files.add(file)
                except (shutil.Error, OSError) as e:
                    print(f"Error moving {file}: {e}")
                pbar.update(1)

if __name__ == '__main__':
    directory = r'C:\Users\Хозяин\Pictures\Screenshots'  # Замените на ваш каталог
    output_dir = r'C:\foto_dupl'  # Замените на ваш каталог для дубликатов

    print(f'Searching for duplicates in: {directory}')
    time_start = datetime.datetime.now()
    duplicates = find_duplicates_multiprocess(directory)
    
    if duplicates:
        print(f'Found {len(duplicates)} duplicate files.')
        move_duplicates(duplicates, output_dir)
        print(f'All duplicates moved to: {output_dir}')
        print(f'Потребовалось {(datetime.datetime.now() - time_start).total_seconds()} секунд')
    else:
        print('No duplicates found.')
        print(f'Потребовалось {(datetime.datetime.now() - time_start).total_seconds()} секунд')
