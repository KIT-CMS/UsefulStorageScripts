#!/usr/bin/env python3.13

import argparse
import os
from XRootD import client
from XRootD.client.flags import DirListFlags, StatInfoFlags

def list_files_recursive(fs, path, output_file, filter_substring=None):
    total_size = 0
    status, listing = fs.dirlist(path, flags=DirListFlags.STAT)
    if not status.ok:
        print(f'Error accessing {path}: {status.message}')
        return total_size
    for item in listing:
        full_path = os.path.join(path, item.name)
        if item.statinfo and item.statinfo.flags & StatInfoFlags.IS_DIR:
            total_size += list_files_recursive(fs, full_path, output_file, filter_substring)
        else:
            if filter_substring is None or filter_substring in full_path:
                output_file.write(f"{full_path} {item.statinfo.size}\n")
                total_size += item.statinfo.size
    return total_size

def human_readable_size(size):
    for unit in ['B', 'kB', 'MB', 'GB', 'TB', 'PB', 'EB', 'ZB', 'YB']:
        if size < 1000:
            return f"{size:.2f} {unit}"
        size /= 1000

def main():
    parser = argparse.ArgumentParser(description='Recursively list files from an XRootD server.')
    parser.add_argument('--server_url', required=True, help='XRootD server URL')
    parser.add_argument('--directory', required=True, help='Directory path on the server to start listing from')
    parser.add_argument('--output_file', default='output.txt', help='File to write the list of files to')
    parser.add_argument('--filter_substring', help='Substring to filter files')

    args = parser.parse_args()

    fs = client.FileSystem(args.server_url)

    with open(args.output_file, 'w') as output_file:
        total_size = list_files_recursive(fs, args.directory, output_file, args.filter_substring)
        print(f"Total size of found files: {human_readable_size(total_size)}")

if __name__ == '__main__':
    main()
