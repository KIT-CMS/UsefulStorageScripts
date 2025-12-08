#! /usr/bin/env python3

import asyncio
import argparse
import os
import logging
import datetime


async def execute_remove(queue, worker, dry_run):
    logger = logging.getLogger()
    logger.info(f"{worker}: Activated")
    dry_run_option = "--dry-run" if dry_run else ""
    interrupted = False
    while not queue.empty() and not interrupted:
        lfn = None
        try:
                lfn, storage_prefix = await queue.get()
                logger.info(f"{worker}: Starting removal process for file {lfn}")
        except asyncio.CancelledError:
            logger.info(f"{worker}: Shutting down due to interruption")
            interrupted = True

        if lfn:
            # For removal we always act on the target storage prefix (single prefix)
            # Build a well-formed URL/path: avoid double slashes
            target_filepath = storage_prefix.rstrip("/") + "/" + lfn.lstrip("/")
            retcode = 1
            command = f"gfal-rm {dry_run_option} {target_filepath}"
            try:
                # retcode 2 corresponds to MISSING file
                while retcode and retcode != 2:
                    logger.info(f"{worker}: Remove command:\n{command}")
                    remove_process = await asyncio.create_subprocess_shell(
                        command,
                        stdout=asyncio.subprocess.PIPE,
                        stderr=asyncio.subprocess.PIPE,
                        stdin=asyncio.subprocess.PIPE,
                    )
                    stdout, stderr = await remove_process.communicate()
                    logger.info(f"{worker}: remove command return code: {remove_process.returncode}")
                    logger.info(f"{worker}: remove command standard output:\n{stdout.decode('utf-8').strip()}")
                    logger.info(f"{worker}: remove command error output:\n{stderr.decode('utf-8').strip()}")
                    retcode = remove_process.returncode
            except asyncio.CancelledError:
                logger.warning(f"{worker}: Cancelling remove command subprocess due to interruption")
                remove_process.terminate()
                interrupted = True

            if not interrupted:
                queue.task_done()
        else:
            continue


async def main(n_threads, dry_run, storage_prefix, filelist):

    logger.info(f"Main: Starting removal process with {len(filelist)} files")
    remove_task_queue = asyncio.Queue()

    for f in filelist:
        logger.info(f"Main: putting {f} in queue")
        remove_task_queue.put_nowait((f, storage_prefix))

    worker_name_template = "remove_worker_{INDEX}"
    remove_workers = []
    for i in range(n_threads):
        worker = asyncio.create_task(
            execute_remove(remove_task_queue, worker_name_template.format(INDEX=i), dry_run)
        )
        remove_workers.append(worker)

    logger.info(f"Main: queue size: {remove_task_queue.qsize()}")
    logger.info(f"Main: workers size: {len(remove_workers)}")

    try:
        logger.info(f"Main: joining queue")
        await remove_task_queue.join()
        logger.info(f"Main: joining queue finished")
    except asyncio.CancelledError:
        logger.warning(f"Main: Caught interruption")
    finally:
        for index, worker in enumerate(remove_workers):
            logger.warning(
                f"Main: Cancelling worker " + worker_name_template.format(INDEX=index)
            )
            worker.cancel()
        await asyncio.gather(*remove_workers, return_exceptions=True)
        logger.info(f"Main: Removal finished")


if __name__ == "__main__":

    parser = argparse.ArgumentParser(
        description="Small script to remove files on a target storage with gfal-rm using a queue-based approach",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--n-threads",
        type=int,
        default=15,
        help="Number of parallel threads to be used for removing.",
    )
    parser.add_argument(
        "--logfile",
        type=str,
        default=f"/ceph/{os.environ['USER']}/logfile_remove_with_gfal.txt",
        help="Path to the logfile used by this script.",
    )
    parser.add_argument("--filelist", required=True, help="Path to the list of logical file names to be removed.")
    parser.add_argument(
        "--storage-prefix",
        required=True,
        help="Storage prefix (target) where the files will be removed, e.g. davs://cmsdcache-kit-disk.gridka.de:2880/pnfs/gridka.de/cms/disk-only",
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
        filename=f"_{datetime.datetime.now():%Y-%m-%d_%H-%M-%S}".join(os.path.splitext(args.logfile))
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
                args.storage_prefix,
                [l.strip().split(",")[0] for l in open(args.filelist, "r").readlines()],
            )
        )
    except asyncio.CancelledError:
        pass
