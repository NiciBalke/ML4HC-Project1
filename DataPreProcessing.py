import pandas as pd
import os
import math
import shutil
from tqdm import tqdm


pathToData = "physionet.org/files/challenge-2012/1.0.0/set-a"
all_data = []
counter = 0

#############################################################
# Go over each file, preprocess is, concat the files and at the end turn into parquet
#
#############################################################
for file in tqdm(os.listdir(pathToData)):
    
    filepath = os.path.join(pathToData, file)

    #ignore the one random .html file that is included in the set-a folder for some reason
    if not filepath.endswith(".txt"): 
        continue
    dataframe = pd.read_csv(filepath)

    dataframe["Time"] = dataframe["Time"].apply(lambda x: int(x[:2]) if (x[3:] == "00") else (1+ int(x[:2])))
    dataframe = dataframe.set_index("Time")
    #print(dataframe.columns)
    #print(dataframe)
    wide_dataframe = dataframe.pivot_table(index = "Time", columns="Parameter", values = "Value")
    wide_dataframe = wide_dataframe.reindex(range(48))
    ##print(wide_dataframe)
    ##print(wide_dataframe.shape[0])
    
    
    for i in range( wide_dataframe.shape[1]):
        currVal = -1
        for j in range(wide_dataframe.shape[0]):
            if(math.isnan(wide_dataframe.iloc[j, i])):
                 wide_dataframe.iloc[j, i] = currVal
            else:
                currVal = wide_dataframe.iloc[j, i]
                
    
    #print("hello")
    wide_dataframe["patient_id"] = counter
    counter +=1
    all_data.append(wide_dataframe)

full_df = pd.concat(all_data, ignore_index=True)
full_df = full_df.ffill().fillna(-1)

#full_df.to_csv("output.txt", sep=",", index=False)

#if parquet file already exists: instead of appending entries to the old file, delete the file such that a new one will be created
output_path = "processedDataProxy.parquet"
if(os.path.exists(output_path)):
    shutil.rmtree(output_path)

full_df.to_parquet("processedDataProxy.parquet", engine="pyarrow", index=False, partition_cols = ["patient_id"])
