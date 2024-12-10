#!/usr/bin/env python3

import argparse
import os
import uproot
import pandas as pd
from natsort import natsorted

def parse_args():
    parser = argparse.ArgumentParser(description="Merge CROWN ntuples and friends.")
    parser.add_argument("--main_directory", required=True, help="Main directory containing ntuples and friends.")
    parser.add_argument("--filelist", required=True, help="File containing list of ROOT files.")
    parser.add_argument("--tree", required=True, help="Name of the tree to process.")
    parser.add_argument("--allowed_friends", nargs='*', help="List of allowed friend trees.")
    return parser.parse_args()

def is_subpath(path, base):
    try:
        common_path = os.path.commonpath([path, base])
        return common_path == base
    except ValueError:
        return False

def get_files(filelist):
    with open(filelist, "r") as f:
        return natsorted([l.strip().split()[0] for l in f.readlines()])

def determine_job_from_file(f, main_ntuples_directory, friends_directory):
    if is_subpath(f, main_ntuples_directory):
        rel_file_path = os.path.relpath(f, main_ntuples_directory)
        job_dir, file_name = os.path.split(rel_file_path)
        return job_dir, "ntuples", f
    elif is_subpath(f, friends_directory):
        rel_file_path = os.path.relpath(f, friends_directory)
        friend_dir, file_name = os.path.split(rel_file_path)
        friend, job_dir = friend_dir.split("/", 1)
        return job_dir, friend, f
    else:
        return "UNKNOWN", "UNKNOWN", f

def check_event_consistency_across_filetypes(job_dict, tree):
    consistency_dict = {}
    for filetype, job_files in job_dict.items():
        for fname in job_files:
            if fname not in consistency_dict:
                consistency_dict[fname] = {}
            with uproot.open(fname) as f:
                t = f[tree]
                consistency_dict[fname][filetype] = t.num_entries
    
    for f, filetype_entries in consistency_dict.items():
        if len(set(filetype_entries.values())) != 1:
            print(f"Error: Inconsistent number of entries in files {f}")
            for filetype, entries in filetype_entries.items():
                print(f"\t{filetype}: {entries}")
            return False
    return True

def merge_ntuples(job, job_dict, tree):
    df_dict = {}
    output_file_name = job.replace("/", "_") + "_merged.root"    
    for filetype, files in job_dict.items():
        if filetype not in df_dict:
            df_dict[filetype] = pd.DataFrame()
        for fname in files:
            with uproot.open(fname) as f:
                t = f[tree]
                df = t.arrays(library="pd")
                df_dict[filetype] = pd.concat([df_dict[filetype], df])
    merged_df = pd.concat(df_dict.values(), axis=1)

    # Write the merged DataFrame to a new ROOT file using uproot
    with uproot.recreate(output_file_name) as f:
        f[tree] = merged_df

if __name__ == "__main__":
    args = parse_args()

    ntuples_directory = os.path.join(args.main_directory, "CROWNRun")
    friends_directory = os.path.join(args.main_directory, "CROWNFriends")

    flist = get_files(args.filelist)

    merge_jobs_dict = {}
    for f in flist:
        job, filetype, file_path = determine_job_from_file(f, ntuples_directory, friends_directory)
        if job not in merge_jobs_dict:
            merge_jobs_dict[job] = {}
        
        if filetype == "ntuples" or filetype in args.allowed_friends:
            if filetype not in merge_jobs_dict[job]:
                merge_jobs_dict[job][filetype] = []
            merge_jobs_dict[job][filetype].append(file_path)

    for job, job_dict in merge_jobs_dict.items():
        print(f"Job: {job}")
        for filetype, files in job_dict.items():
            merge_jobs_dict[job][filetype] = natsorted(files)
            print(f"\t{filetype}:")
            for f in merge_jobs_dict[job][filetype]:
                print(f"\t\t{f}")

    for job, job_dict in merge_jobs_dict.items():
        if not check_event_consistency_across_filetypes(job_dict, args.tree):
            print(f"Error: Inconsistent number of events across filetypes for job {job}")
            exit(1)
        else:
            print(f"Job {job} is consistent in number of events across filetypes")
            merge_ntuples(job, job_dict, args.tree)
            print(f"Merged file created for job {job}")