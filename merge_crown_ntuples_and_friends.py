#!/usr/bin/env python3

import argparse
import os
import uproot
import pandas as pd
from natsort import natsorted
import asyncio
import logging
import datetime

def parse_args():
    parser = argparse.ArgumentParser(description="Merge CROWN ntuples and friends.")
    parser.add_argument("--main_directory", required=True, help="Main directory containing ntuples and friends.")
    parser.add_argument("--filelist", required=True, help="File containing list of ROOT files.")
    parser.add_argument("--tree", required=True, help="Name of the tree to process.")
    parser.add_argument("--allowed_friends", nargs='*', default=[], help="List of allowed friend trees.")
    parser.add_argument("--n_threads", type=int, default=4, help="Number of parallel threads to be used for merging.")
    parser.add_argument("--logfile", type=str, default="logfile_merge.txt", help="Path to the logfile used by this script.")
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

async def merge_ntuples(queue, worker_id, tree):
    logger = logging.getLogger(f"worker_{worker_id}")
    logger.info(f"Worker {worker_id}: Activated")
    while True:
        job, job_dict = await queue.get()
        if job is None:
            queue.task_done()
            break
        logger.info(f"Worker {worker_id}: Starting merging process for job {job}")
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

        logger.info(f"Worker {worker_id}: Merged file created for job {job}")
        queue.task_done()

async def main():
    args = parse_args()

    logging.getLogger("asyncio").setLevel(logging.NOTSET)

    logger = logging.getLogger()
    logger.setLevel(logging.INFO)

    formatter = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s")

    stream_handler = logging.StreamHandler()
    stream_handler.setLevel(logging.INFO)
    stream_handler.setFormatter(formatter)

    infofile_handler = logging.FileHandler(
        filename=f"_{datetime.datetime.now():%Y-%m-%d_%H-%M-%S}".join(
            os.path.splitext(args.logfile)
        )
    )
    infofile_handler.setFormatter(formatter)
    infofile_handler.setLevel(logging.INFO)

    logger.addHandler(stream_handler)
    logger.addHandler(infofile_handler)

    main_ntuples_directory = os.path.join(args.main_directory, "CROWNRun")
    friends_directory = os.path.join(args.main_directory, "CROWNFriends")

    flist = get_files(args.filelist)

    merge_jobs_dict = {}
    for f in flist:
        job, filetype, file_path = determine_job_from_file(f, main_ntuples_directory, friends_directory)
        if job not in merge_jobs_dict:
            merge_jobs_dict[job] = {}
        
        if filetype == "ntuples" or filetype in args.allowed_friends:
            if filetype not in merge_jobs_dict[job]:
                merge_jobs_dict[job][filetype] = []
            merge_jobs_dict[job][filetype].append(file_path)

    for job, job_dict in merge_jobs_dict.items():
        logger.info(f"Main: Job: {job}")
        for filetype, files in job_dict.items():
            merge_jobs_dict[job][filetype] = natsorted(files)
            logger.info(f"Main: \t{filetype}:")
            for f in merge_jobs_dict[job][filetype]:
                logger.info(f"Main: \t\t{f}")

    merge_task_queue = asyncio.Queue()

    worker_name_template = "merge_worker_{INDEX}"
    merge_workers = []
    nworkers = min(len(merge_jobs_dict), args.n_threads)
    for i in range(nworkers):
        worker = asyncio.create_task(
            merge_ntuples(merge_task_queue, worker_name_template.format(INDEX=i), args.tree)
        )
        merge_workers.append(worker)

    logger.info(f"Main: workers size: {len(merge_workers)}")

    for job, job_dict in merge_jobs_dict.items():
        if not check_event_consistency_across_filetypes(job_dict, args.tree):
            logger.error(f"Main: Inconsistent number of events across filetypes for job {job}")
            exit(1)
        else:
            logger.info(f"Main: Job {job} is consistent in number of events across filetypes")
            await merge_task_queue.put((job, job_dict))
    logger.info(f"Main: queue size: {merge_task_queue.qsize()}")

    logger.info(f"Main: joining queue")
    await merge_task_queue.join()
    logger.info(f"Main: joining queue finished")
    for _ in range(nworkers):
        await merge_task_queue.put((None, None))
    await asyncio.gather(*merge_workers)
    logger.info(f"Main: Merging finished")

if __name__ == "__main__":
    asyncio.run(main())