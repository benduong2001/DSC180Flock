# -*- coding: utf-8 -*-
"""run.ipynb

Automatically generated by Colaboratory.

Original file is located at
    https://colab.research.google.com/drive/1hXXFKBFey6VMafTomACoYCXae9GSCxyD
"""

#!pip install geopandas

#!/usr/bin/env python
# coding: utf-8

# In[1]:


import sys
import os

google_colab = 0
if google_colab == 1:
    # Load the Drive helper and mount
    from google.colab import drive
    mounted_path_folder = '/content/drive'
    drive.mount(mounted_path_folder, force_remount=True)
    path_folder = "/content/drive/MyDrive/dsprojects/dsproject_ff_geo/current/" # parent of current src folder
else:
    path_folder = os.getcwd()

sys.path.insert(0, path_folder)

import src.etl.make_dataset as etl
import src.dataclean.build_features as features
import src.etl.create_test_data as create_test_data

import src.models.model as model
import os

import numpy as np
import pandas as pd

def main_etl(args):
    import src.etl.make_dataset as etl
    etl.main(args)


def main_dataclean(args):
    #data_clean_first_time = 1
    features.main(args)
    do_workflow = 1
    if do_workflow == 1:
        import pipeline
        pipeline.main(args)

    

def main_model(args):
    model.main(args)

def main_create_test_data(args):
    print("to create test data")
    create_test_data.main(args)

def main_test(args):
    main_clear(args)
    main_create_test_data(args)
    main_dataclean(args)
    #main_model(args)    

def main_all(args):
    main_clear(args)
    main_etl(args)
    main_dataclean(args)
    main_model(args)

def main_clear(args):
    path_folder_data = args["path_folder_data"]
    path_folder_data_raw = os.path.join(path_folder_data,"raw")
    path_folder_data_temp = os.path.join(path_folder_data,"temp")
    path_folder_data_out = os.path.join(path_folder_data,"out")
    path_folders_data = [path_folder_data_raw,path_folder_data_temp,path_folder_data_out]
    for path_folder_i_data in path_folders_data:
        for filename in os.listdir(path_folder_i_data):
            if filename in [".gitignore.txt", "stub.txt"]:
                print("gitignore")
                continue
            path_file = os.path.join(path_folder_i_data, filename)
            try:
                os.remove(path_file)
            except:
                try:
                    path_inner_folder = path_file
                    for file_name in os.listdir(path_inner_folder):
                        # construct full file path
                        file = os.path.join(path_inner_folder, file_name)
                        if os.path.isfile(file):
                            os.remove(file)
                    os.rmdir(path_inner_folder)
                except:
                    pass
        print(os.listdir(path_folder_i_data))

def main(targets):
    #base_path_folder = "../../"
    #path_folder = os.getcwd()
    path_folder_data = os.path.join(path_folder,"data")
    test_path_folder = os.path.join(path_folder,"test")
    test_path_folder_data = os.path.join(test_path_folder,"test-data")
    
    args = {
        "path_folder": path_folder,
        "path_folder_data": path_folder_data,
        "file_name_temp_avg_stdev": "temp_avg_stdev.csv",
        "file_name_temp_amount": "temp_amount.csv",
        "file_name_final_df": "final_df.csv",
        #"temp_foreign_key_column_name": "REFERENCE",
    }
    for target in targets:
        if target in ["test"]:
            # revamp
            args["path_folder_data"] = test_path_folder_data
            main_test(args)
        if target in ["all"]:
            main_all(args)
        
        if target in ["data"]:
            main_etl(args)
        if target in ["features"]:
            main_dataclean(args)
        if target in ["model"]:
            main_model(args)
        if target in ["clear"]:
            main_clear(args)
        if target in ["test-data"]:
            args["path_folder_data"] = test_path_folder_data




if __name__ == "__main__":
    targets = sys.argv[1:]
    #targets = ["test"]
    #targets = ["all"]
    main(targets)

