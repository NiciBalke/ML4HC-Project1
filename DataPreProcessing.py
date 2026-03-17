import pandas as pd
import os
from tqdm import tqdm
import shutil

pathToData = "physionet.org/files/challenge-2012/1.0.0/set-a"
output_path = "physionet.org/files/challenge-2012/1.0.0/Outcomes-a.txt" # Assuming outcomes are here

all_data = []

# Load the outcomes first so we can merge them later
outcomes_df = pd.read_csv(output_path)
# Keep only the ID and our target label
outcomes_df = outcomes_df[['RecordID', 'In-hospital_death']]

for file in tqdm(os.listdir(pathToData)):
    filepath = os.path.join(pathToData, file)

    if not filepath.endswith(".txt"): 
        continue
        
    # Extract the actual RecordID from the filename (e.g., "132539.txt" -> 132539)
    record_id = int(file.replace(".txt", ""))

    dataframe = pd.read_csv(filepath)

    # Round time UP to preserve causality
    dataframe["Time"] = dataframe["Time"].apply(lambda x: int(x[:2]) if (x[3:] == "00") else (1 + int(x[:2])))
    
    # Pivot the table
    wide_dataframe = dataframe.pivot_table(index="Time", columns="Parameter", values="Value")
    
    # Reindex to 49 steps (0 to 48 inclusive)
    wide_dataframe = wide_dataframe.reindex(range(49))
    
    # Add PatientID using the actual record_id
    wide_dataframe["PatientID"] = record_id
    
    # Reset index so 'Time' becomes a normal column instead of the index
    wide_dataframe = wide_dataframe.reset_index()
    
    all_data.append(wide_dataframe)

# Concatenate all patient dataframes
full_df = pd.concat(all_data, ignore_index=True)

# Merge the outcomes based on the RecordID
# This will attach the 'In-hospital_death' column to every row for a given patient
full_df = full_df.merge(outcomes_df, left_on='PatientID', right_on='RecordID', how='left')

output_path = "processedDataProxy.parquet"

print(full_df.head())
if os.path.exists(output_path):
    if os.path.isdir(output_path):
        shutil.rmtree(output_path)  # Delete if it's a folder
    else:
        os.remove(output_path)      # Delete if it's a file

# Save to parquet
full_df.to_parquet("processedDataProxy.parquet", engine="pyarrow", index=False)