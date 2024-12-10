#!/usr/bin/env python3

import argparse
import os
import ROOT as r
import uproot
import numpy as np
import pandas as pd
from natsort import natsorted


main_directory = "input_files/CROWN/ntuples/11_07_24_alleras_allch/"
main_ntuples_directory = os.path.join(main_directory, "CROWNRun")
friends_directory = os.path.join(main_directory, "CROWNFriends")
filelist = "rschmieder_files_Run2018CD_SingleMuon_mmt_local.txt"
tree = "ntuple"
allowed_friends = [
    "crosssection",
    "jetfakes_wpVSjet_Loose_30_08_24_LoosevsJetsvsL",
    "jetfakes_wpVSjet_Loose_11_10_24_LoosevsJetsvsL_measure_njetclosure",
    "jetfakes_wpVSjet_Loose_11_10_24_LoosevsJetsvsL_measure_metclosure",
    "met_unc_22_10_24",
    "pt_1_unc_22_10_24",
    "nn_friends_18_07_24_LoosevsJL",
]

def is_subpath(path, base):
    try:
        common_path = os.path.commonpath([path, base])
        return common_path == base
    except ValueError:
        return False

def get_files(filelist):
    with open(filelist, "r") as f:
        return [l.strip().split()[0] for l in f.readlines()]


def determine_job_from_file(f):
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

def check_event_consistency_across_filetypes(job_dict):
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

def merge_ntuples(job, job_dict):
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
    with uproot.recreate(output_file_name) as out:
        out[tree] = merged_df

if __name__ == "__main__":
    flist = get_files(filelist)

    merge_jobs_dict = {}
    for f in flist:
        job, filetype, file_path = determine_job_from_file(f)
        if job not in merge_jobs_dict:
            merge_jobs_dict[job] = {}
        
        if filetype == "ntuples" or filetype in allowed_friends:
            if filetype not in merge_jobs_dict[job]:
                merge_jobs_dict[job][filetype] = []
            merge_jobs_dict[job][filetype].append(file_path)

    for job, job_dict in merge_jobs_dict.items():
        print (f"Job: {job}")
        for filetype, files in job_dict.items():
            merge_jobs_dict[job][filetype] = natsorted(files)
            print(f"\t{filetype}:")
            for f in merge_jobs_dict[job][filetype]:
                print(f"\t\t{f}")

    for job, job_dict in merge_jobs_dict.items():
        if not check_event_consistency_across_filetypes(job_dict):
            print("Error: Inconsistent number of events across filetypes for job {job}")
            exit(1)
        else:
            print(f"Job {job} is consistent in number of events across filetypes")
            merge_ntuples(job, job_dict)