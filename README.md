# UsefulStorageScripts

## Example command

### merge_crown_ntuples_and_friends.py

A collection of files stored in `filelist.txt` containing `.root` files of the form `/store/user/${USER}/CROWN/ntuples/<TAG>/CROWNRun/<ERA>/<SAMPLE>/<CHANNEL>/*.root` can be merged, including possible friends, by using

```bash
python3 merge_crown_ntuples_and_friends.py \
  --main_directory /store/user/${USER}/CROWN/ntuples/<TAG>/ \
  --filelist filelist.txt \
  --tree ntuple \
  --allowed_friends <first_friend_name> <second_friend_name> <third_friend_name> \
  --remote_server root://xrootd-cms.infn.it/ \
  --run_nevents_check \
  --n_threads 4
```

`--remote_server` should be specified to the XRootD endpoint where the files are stored accordingly (e.g. `root://cmsdcache-kit-disk.gridka.de`), or alternatively, to a redirector able to figure this out (e.g. `root://xrootd-cms.infn.it`).

## remove_files.py

A utility was added to remove files on target storage using `gfal-rm`. It mirrors the behavior of `copy_files.py` but performs removals instead of copies. Example usage (use the davs PNFS path as your storage prefix):

```bash
python3 remove_files.py \
  --filelist filelist.txt \
  --storage-prefix davs://cmsdcache-kit-disk.gridka.de:2880/pnfs/gridka.de/cms/disk-only \
  --n-threads 10 --dry-run
```

## stage_files.py

Manages large-scale tape staging against a dCache instance via the WLCG Tape REST API v1. Handles the full lifecycle: locality pre-check, batch submission, progress polling, and automatic release (unpin) of completed files.

**Requirements:** Python 3.12+, `requests` library (`pip install requests`).

**Usage:**

```bash
python3 stage_files.py /path/to/workdir
```

The workdir must contain exactly one `.conf` file and a file list. Example `.conf`:

```ini
[dcache]
base_url = https://dcache-host.example.org:3880
api_path = /api/v1/tape
batch_size = 2000
disk_lifetime = P7D
poll_interval = 300
archiveinfo_batch_size = 5000
auto_release = true

[auth]
# Falls back to $X509_USER_PROXY / /tmp/x509up_u<uid>
proxy_cert =
# Falls back to $X509_CERT_DIR / /etc/grid-security/certificates
ca_dir =
# Stop gracefully when remaining proxy lifetime < factor × poll_interval (default: 2.0)
proxy_lifetime_factor = 2.0

[files]
filelist = filelist.txt
state_file = stage_state.json

[logging]
log_file = stage_files.log
```

State is persisted to a JSON file (`stage_state.json`) so the script can be killed and restarted at any time. On restart it resumes from the last saved phase. Completed files are automatically released (unpinned) to free staging pool space, which is essential when the total file size exceeds the pool capacity.

The script monitors the X.509 proxy certificate lifetime and shuts down gracefully before it expires (configurable via `proxy_lifetime_factor`). HTTP 401/403 errors are also caught and trigger a clean shutdown with state saved.

**Dependencies:** `requests`, `cryptography` (`pip install requests cryptography`).