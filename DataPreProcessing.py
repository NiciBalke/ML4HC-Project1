import pandas as pd
import os
import math


pathToData = "dataSetProxy"
all_data = []
counter = 0

#############################################################
# Go over each file, preprocess is, concat the files and at the end turn into parquet
#
#############################################################
for file in os.listdir(pathToData):
    
    filepath = os.path.join(pathToData, file)
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

full_df.to_parquet("processedDataProxy.parquet", engine="pyarrow", index=False, partition_cols = ["patient_id"])
