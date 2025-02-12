#! /usr/bin/env python3

import asyncio
import argparse
import shlex
import os
import logging
import datetime
import uuid
import itertools


async def execute_copy(queue, worker, dry_run):
    logger = logging.getLogger()
    logger.info(f"{worker}: Activated")
    dry_run_option = "--dry-run" if dry_run else ""
    interrupted = False
    while not queue.empty() and not interrupted:
        source_lfn = None
        try:
            # now tuple contains: (source_lfn, new_name, input_prefix, output_prefix, old_directory, new_directory)
            source_lfn, new_name, input_prefix, output_prefix, old_directory, new_directory = await queue.get()
            input_filepath = os.path.join(input_prefix, source_lfn.lstrip("/"))
            logger.info(f"{worker}: Starting copying process for file {source_lfn}")
        except asyncio.CancelledError:
            logger.info(f"{worker}: Shutting down due to interruption")
            interrupted = True

        if source_lfn:
            # Construct new target filepath based solely on the new name and new_directory:
            target_filepath = os.path.join(output_prefix, new_directory, new_name)
            retcode = 1
            command = f"gfal-copy {dry_run_option} --force {input_filepath} {target_filepath} --checksum-mode both"
            try:
                while retcode:
                    logger.info(f"{worker}: Copy command:\n{command}")
                    copy_process = await asyncio.create_subprocess_shell(
                        command,
                        stdout=asyncio.subprocess.PIPE,
                        stderr=asyncio.subprocess.PIPE,
                        stdin=asyncio.subprocess.PIPE,
                    )
                    stdout, stderr = await copy_process.communicate()
                    logger.info(f"{worker}: copy command return code: {copy_process.returncode}")
                    logger.info(f"{worker}: copy command standard output:\n{stdout.decode('utf-8').strip()}")
                    logger.info(f"{worker}: copy command error output:\n{stderr.decode('utf-8').strip()}")
                    retcode = copy_process.returncode
                    if retcode != 0:
                        logger.error(f"{worker}: copy command failed for {source_lfn}, trying to remove the file from target site.")
                        remove_command = f"gfal-rm {target_filepath}"
                        logger.info(f"{worker}: Remove command:\n{remove_command}")
                        remove_process = await asyncio.create_subprocess_shell(
                            remove_command,
                            stdout=asyncio.subprocess.PIPE,
                            stderr=asyncio.subprocess.PIPE,
                            stdin=asyncio.subprocess.PIPE,
                        )
                        remove_stdout, remove_stderr = await remove_process.communicate()
                        logger.info(f"{worker}: remove command return code: {remove_process.returncode}")
                        logger.info(f"{worker}: remove command standard output:\n{remove_stdout.decode('utf-8').strip()}")
                        logger.info(f"{worker}: remove command error output:\n{remove_stderr.decode('utf-8').strip()}")
            except asyncio.CancelledError:
                logger.warning(f"{worker}: Cancelling copy command subprocess due to interruption")
                copy_process.terminate()
                interrupted = True

            if not interrupted:
                queue.task_done()
        else:
            continue


async def main(n_threads, dry_run, old_directory, new_directory, input_prefix, output_prefix, filelist, total_transfers, extension):
    logger = logging.getLogger()
    logger.info(f"Main: Starting copying process with total transfers {total_transfers}")
    copy_task_queue = asyncio.Queue()

    file_cycle = itertools.cycle(filelist)
    for _ in range(total_transfers):
        source_file = next(file_cycle)
        new_name = f"{uuid.uuid4()}{extension}"
        logger.info(f"Main: Queuing transfer from {source_file} -> {new_name}")
        # Note: you can still pass old_directory/new_directory if needed elsewhere
        copy_task_queue.put_nowait((source_file, new_name, input_prefix, output_prefix, old_directory, new_directory))

    worker_name_template = "copy_worker_{INDEX}"
    copy_workers = []
    for i in range(n_threads):
        worker = asyncio.create_task(
            execute_copy(copy_task_queue, worker_name_template.format(INDEX=i), dry_run)
        )
        copy_workers.append(worker)

    logger.info(f"Main: queue size: {copy_task_queue.qsize()}")
    logger.info(f"Main: workers size: {len(copy_workers)}")

    try:
        logger.info("Main: joining queue")
        await copy_task_queue.join()
        logger.info("Main: queue join finished")
    except asyncio.CancelledError:
        logger.warning("Main: Caught interruption")
    finally:
        for index, worker in enumerate(copy_workers):
            logger.warning("Main: Cancelling worker " + worker_name_template.format(INDEX=index))
            worker.cancel()
        await asyncio.gather(*copy_workers, return_exceptions=True)
        logger.info("Main: Copying finished")


if __name__ == "__main__":

    parser = argparse.ArgumentParser(
        description="Script to copy files from one folder to another with gfal-copy using a queue-based approach, "
                    "with the option to transfer test files a limited number of times with unique filenames.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("--n-threads", type=int, default=15, help="Number of parallel threads to be used for copying.")
    parser.add_argument(
        "--logfile",
        type=str,
        default=f"/ceph/{os.environ['USER']}/logfile_copy_with_gfal.txt",
        help="Path to the logfile used by this script.",
    )
    parser.add_argument("--filelist", required=True, help="Path to the list of logical file names to be copied.")
    parser.add_argument("--old-directory", required=True, help="Old directory to be replaced in file paths.")
    parser.add_argument("--new-directory", required=True, help="New directory to use in target file paths.")
    parser.add_argument("--input-storage-prefix", required=True, help="Storage prefix for input files")
    parser.add_argument("--output-storage-prefix", required=True, help="Storage prefix for output files")
    parser.add_argument("--dry-run", action="store_true", help="Run in dry-run mode for testing purposes")
    # New parameters
    parser.add_argument("--total-transfers", type=int, default=1000,
                        help="Total number of transfers to perform.")
    parser.add_argument("--extension", type=str, default=".file",
                        help="Extension to append to the new UUID-based basenames.")

    args = parser.parse_args()

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

    # Read filelist and take the first field (as before)
    filelist = [l.strip().split(",")[0] for l in open(args.filelist, "r").readlines()]

    try:
        asyncio.run(
            main(
                args.n_threads,
                args.dry_run,
                args.old_directory,
                args.new_directory,
                args.input_storage_prefix,
                args.output_storage_prefix,
                filelist,
                args.total_transfers,
                args.extension,
            )
        )
    except asyncio.CancelledError:
        pass
