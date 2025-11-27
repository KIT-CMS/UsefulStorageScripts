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