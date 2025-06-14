import os
import shutil
import datetime
import multiprocessing
from collections import defaultdict
from tqdm import tqdm


def organize_by_date_threshold(directory, output_dir, threshold=5):
    os.makedirs(output_dir, exist_ok=True)
    files_by_date = defaultdict(list)
    files = []
    for root, _, filenames in os.walk(directory):
        files.extend([os.path.join(root, f) for f in filenames])

    with multiprocessing.Pool() as pool, tqdm(total=len(files), desc="Collecting file dates") as pbar:
        for filepath, creation_date in pool.imap_unordered(get_creation_date_wrapper, files):
            if creation_date:
                files_by_date[creation_date.date()].append(filepath)
            pbar.update(1)

    with tqdm(total=len(files_by_date), desc="Organizing files") as pbar:
        for date, filepaths in files_by_date.items():
            if len(filepaths) >= threshold:
                date_str = date.strftime('%d.%m.%Y')  # Полная дата
            else:
                date_str = date.strftime('%Y')  # Только год

            dest_dir = os.path.join(output_dir, date_str)
            os.makedirs(dest_dir, exist_ok=True)

            for filepath in filepaths:
                dest_path = os.path.join(dest_dir, os.path.basename(filepath))
                try:
                    shutil.move(filepath, dest_path)
                except (shutil.Error, OSError) as e:
                    print(f"Error moving {filepath}: {e}")
            pbar.update(1)


def get_creation_date_wrapper(filepath):
    try:
        if os.path.isfile(filepath):
            return filepath, get_creation_date(filepath)
        else:
            return filepath, None
    except (IOError, OSError):
        return filepath, None

def get_creation_date(filepath):
    timestamp = os.path.getmtime(filepath)
    return datetime.datetime.fromtimestamp(timestamp)


if __name__ == '__main__':
    directory = r'C:\foto_dupl'  # Замените на ваш каталог
    output_dir = r'C:\foto_sorted'  # Замените на ваш каталог для отсортированных файлов

    print(f'Organizing files from: {directory}')
    time_start = datetime.datetime.now()
    organize_by_date_threshold(directory, output_dir)
    print(f'Files organized and moved to: {output_dir}')
    print(f'Потребовалось {datetime.datetime.now() - time_start} времени')


# import os
# import shutil
# import datetime
# import multiprocessing
# from tqdm import tqdm

# def organize_by_month(directory, output_dir):
#     os.makedirs(output_dir, exist_ok=True)
#     files = []
#     for root, _, filenames in os.walk(directory):
#         files.extend([os.path.join(root, f) for f in filenames])

#     with multiprocessing.Pool() as pool, tqdm(total=len(files), desc="Organizing files") as pbar:
#         for filepath, creation_date in pool.imap_unordered(get_creation_date_wrapper, files):
#             if creation_date:
#                 date_str = creation_date.strftime('%Y-%m')  # Формат год-месяц
#                 dest_dir = os.path.join(output_dir, date_str)
#                 os.makedirs(dest_dir, exist_ok=True)
#                 dest_path = os.path.join(dest_dir, os.path.basename(filepath))
#                 try:
#                     shutil.move(filepath, dest_path)
#                 except (shutil.Error, OSError) as e:
#                     print(f"Error moving {filepath}: {e}")
#             pbar.update(1)


# def get_creation_date_wrapper(filepath):
#     try:
#         if os.path.isfile(filepath):
#             return filepath, get_creation_date(filepath)
#         else:
#             return filepath, None
#     except (IOError, OSError):
#         return filepath, None

# def get_creation_date(filepath):
#     timestamp = os.path.getmtime(filepath)
#     return datetime.datetime.fromtimestamp(timestamp)


# if __name__ == '__main__':
#     directory = r'C:\foto_dupl'  # Замените на ваш каталог
#     output_dir = r'C:\foto_sorted'  # Замените на ваш каталог для отсортированных файлов

#     print(f'Organizing files from: {directory}')
#     time_start = datetime.datetime.now()
#     organize_by_month(directory, output_dir)  # Используем новую функцию
#     print(f'Files organized and moved to: {output_dir}')
#     print(f'Потребовалось {datetime.datetime.now() - time_start} времени')


# import os
# import hashlib
# import shutil
# import datetime
# import multiprocessing
# from tqdm import tqdm

# def find_duplicates_multiprocess(directory):
#     hashes = {}
#     duplicates = []
#     files = []
#     for root, _, filenames in os.walk(directory):
#         files.extend([os.path.join(root, f) for f in filenames])

#     with multiprocessing.Pool() as pool, tqdm(total=len(files), desc="Hashing files") as pbar:
#         for filepath, file_hash in pool.imap_unordered(hash_file_wrapper, files):
#             if file_hash:
#                 if file_hash in hashes:
#                     duplicates.append(filepath)
#                 else:
#                     hashes[file_hash] = filepath
#             pbar.update(1)
#     return duplicates

# def hash_file_wrapper(filepath):
#     try:
#         if os.path.isfile(filepath):
#             return filepath, hash_file(filepath)
#         else:
#             return filepath, None
#     except (IOError, OSError):
#         return filepath, None

# def hash_file(filepath):
#     hasher = hashlib.sha256()
#     with open(filepath, 'rb') as f:
#         while True:
#             chunk = f.read(8192)
#             if not chunk:
#                 break
#             hasher.update(chunk)
#     return hasher.hexdigest()

# def move_duplicates(duplicates, output_dir):
#     os.makedirs(output_dir, exist_ok=True)
#     moved_files = set()
#     with tqdm(total=len(duplicates), desc="Moving duplicates") as pbar:
#         for file in duplicates:
#             if file not in moved_files:
#                 dest = os.path.join(output_dir, os.path.basename(file))
#                 try:
#                     shutil.move(file, dest)
#                     moved_files.add(file)
#                 except (shutil.Error, OSError) as e:
#                     print(f"Error moving {file}: {e}")
#                 pbar.update(1)

# if __name__ == '__main__':
#     directory = r'D:\Локальный диск F\Картинки\МОЙ CANON'  # Замените на ваш каталог
#     output_dir = r'C:\foto_dupl'  # Замените на ваш каталог для дубликатов

#     print(f'Searching for duplicates in: {directory}')
#     time_start = datetime.datetime.now()
#     duplicates = find_duplicates_multiprocess(directory)
    
#     if duplicates:
#         print(f'Found {len(duplicates)} duplicate files.')
#         move_duplicates(duplicates, output_dir)
#         print(f'All duplicates moved to: {output_dir}')
#         print(f'Потребовалось {datetime.datetime.now() - time_start} времени')
#     else:
#         print('No duplicates found.')
#         print(f'Потребовалось {datetime.datetime.now() - time_start} времени')
