#! /usr/bin/env python3

import asyncio
import argparse
import shlex
import os
import logging
import datetime


async def execute_copy(queue, worker, dry_run):
    logger = logging.getLogger()
    logger.info(f"{worker}: Activated")
    dry_run_option = "--dry-run" if dry_run else ""
    interrupted = False
    while not queue.empty() and not interrupted:
        lfn = None
        try:
            lfn, input_storage_prefix, output_storage_prefix, olddir, newdir = await queue.get()
            input_filepath = input_storage_prefix+lfn
            logger.info(f"{worker}: Starting copying process for file {lfn}")
        except asyncio.CancelledError:
            logger.info(f"{worker}: Shutting down due to interruption")
            interrupted = True

        if lfn:
            target_filepath = output_storage_prefix+lfn.replace(olddir, newdir)
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
                    logger.info(
                        f"{worker}: copy command return code: {copy_process.returncode}"
                    )
                    logger.info(
                        f"{worker}: copy command standard output:\n{stdout.decode('utf-8').strip()}"
                    )
                    logger.info(
                        f"{worker}: copy command error output:\n{stderr.decode('utf-8').strip()}"
                    )
                    retcode = copy_process.returncode
                    if retcode != 0:
                        logger.error(
                            f"{worker}: copy command failed for {lfn}, trying to remove the file from target site."
                        )
                        remove_command = f"gfal-rm {target_filepath}"
                        logger.info(f"{worker}: Remove command:\n{remove_command}")
                        remove_process = await asyncio.create_subprocess_shell(
                            remove_command,
                            stdout=asyncio.subprocess.PIPE,
                            stderr=asyncio.subprocess.PIPE,
                            stdin=asyncio.subprocess.PIPE,
                        )
                        remove_stdout, remove_stderr = await remove_process.communicate()
                        logger.info(
                            f"{worker}: remove command return code: {remove_process.returncode}"
                        )
                        logger.info(
                            f"{worker}: remove command standard output:\n{remove_stdout.decode('utf-8').strip()}"
                        )
                        logger.info(
                            f"{worker}: remove command error output:\n{remove_stderr.decode('utf-8').strip()}"
                        )
            except asyncio.CancelledError:
                logger.warning(
                    f"{worker}: Cancelling copy command subprocess due to interruption"
                )
                copy_process.terminate()
                interrupted = True

            if not interrupted:
                queue.task_done()
        else:
            continue


async def main(n_threads, dry_run, old_directory, new_directory, input_storage_prefix, output_storage_prefix, filelist):

    logger.info(f"Main: Starting copying process with {len(filelist)} files")
    copy_task_queue = asyncio.Queue()

    for f in filelist:
        logger.info(f"Main: putting {f} in queue")
        copy_task_queue.put_nowait((f, input_storage_prefix, output_storage_prefix, old_directory, new_directory))

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
        logger.info(f"Main: joining queue")
        await copy_task_queue.join()
        logger.info(f"Main: joining queue finished")
    except asyncio.CancelledError:
        logger.warning(f"Main: Caught interruption")
    finally:
        for index, worker in enumerate(copy_workers):
            logger.warning(
                f"Main: Cancelling worker " + worker_name_template.format(INDEX=index)
            )
            worker.cancel()
        await asyncio.gather(*copy_workers, return_exceptions=True)
        logger.info(f"Main: Copying finished")


if __name__ == "__main__":

    parser = argparse.ArgumentParser(
        description="Small script to copy files from one folder to another with gfal-copy using a queue-based approach",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--n-threads",
        type=int,
        default=15,
        help="Number of parallel threads to be used for copying.",
    )
    parser.add_argument(
        "--logfile",
        type=str,
        default=f"/ceph/{os.environ["USER"]}/logfile_copy_with_gfal.txt",
        help="Path to the logfile used by this script.",
    )
    parser.add_argument(
        "--filelist", required=True, help="Path to the list of logical file names to be copied."
    )
    parser.add_argument(
        "--old-directory",
        required=True,
        help="Old directory, which is replace by a new path. Must be contained in the full path of files.",
    )
    parser.add_argument(
        "--new-directory",
        required=True,
        help="New directory used to replace the old one.",
    )
    parser.add_argument(
        "--input-storage-prefix",
        required=True,
        help="storage prefix to be used for the files on input storage",
    )
    parser.add_argument(
        "--output-storage-prefix",
        required=True,
        help="storage prefix to be used for the files on output storage",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Running in dry-run mode for testing purposes",
    )

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

    try:
        asyncio.run(
            main(
                args.n_threads,
                args.dry_run,
                args.old_directory,
                args.new_directory,
                args.input_storage_prefix,
                args.output_storage_prefix,
                [l.strip().split(",")[0] for l in open(args.filelist, "r").readlines()],
            )
        )
    except asyncio.CancelledError:
        pass
