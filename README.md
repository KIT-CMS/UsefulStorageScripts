# UsefulStorageScripts


## Example command

```bash
python3 merge_crown_ntuples_and_friends.py \
  --main_directory input_files/CROWN/ntuples/11_07_24_alleras_allch/ \
  --filelist rschmieder_files_Run2018CD_SingleMuon_mmt_local.txt \
  --tree ntuple --allowed_friends crosssection jetfakes_wpVSjet_Loose_30_08_24_LoosevsJetsvsL \
                                  jetfakes_wpVSjet_Loose_11_10_24_LoosevsJetsvsL_measure_njetclosure \
                                  jetfakes_wpVSjet_Loose_11_10_24_LoosevsJetsvsL_measure_metclosure \
                                  met_unc_22_10_24 pt_1_unc_22_10_24 nn_friends_18_07_24_LoosevsJL
```
