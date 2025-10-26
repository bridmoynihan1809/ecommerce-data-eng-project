import hashlib
import os


def get_md5(file_path: str):
    """
    Computes the MD5 hash of a file.
    """
    hash = hashlib.md5()
    with open(file_path, 'rb') as file:
        while chunk := file.read(4096):
            hash.update(chunk)
    return hash.hexdigest()


def extract_file_name(csv_file: str):
    """
    Extracts the file name without the extension from a given file path.
    """
    file_name_ext = (csv_file.split('/')[-1]).replace(" ", "_")
    return os.path.splitext(file_name_ext)[0]
