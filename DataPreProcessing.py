import pandas as pd
import os
from tqdm import tqdm
import shutil

for set in ["a", "b", "c"]:  
    for imputstr in ["imputed", "not-imputed"]:
        which_dataset:str = set # which physionet set to use
        imputeds:str = imputstr
        imputed: bool = (imputeds == "imputed")
        output_path = f"parquet_files/processedDataProxy-{which_dataset}-{imputeds}.parquet"

        pathToData :str= f"physionet.org/files/challenge-2012/1.0.0/set-{which_dataset}" #containin 4000 patient files
        outcome_path:str = f"physionet.org/files/challenge-2012/1.0.0/Outcomes-{which_dataset}.txt" # Assuming outcomes are here

        all_data:list = []

        # Load the outcomes first so we can merge them later
        outcomes_df:pd.DataFrame= pd.read_csv(outcome_path)
        # Keep only the ID and our target label
        outcomes_df = outcomes_df[['RecordID', 'In-hospital_death']] #filter it to only two columns
        file:str
        for file in tqdm(os.listdir(pathToData)): #os.listdir(pathToData) looks pathToData's directory and takes a list of all filenames in there, then tqdm is for progress bar
            filepath:str = os.path.join(pathToData, file) #the entire filetext

            if not filepath.endswith(".txt"): 
                continue
                
            # Extract the actual RecordID from the filename (e.g., "132539.txt" -> 132539)
            record_id = int(file.replace(".txt", "")) #file is a string and we take the txt away

            dataframe:pd.DataFrame = pd.read_csv(filepath) #we read for each patient the file in a separate dataframe

            # Round time UP to preserve causality
            dataframe["Time"] = dataframe["Time"].apply(lambda x: int(x[:2]) if (x[3:] == "00") else (1 + int(x[:2])))
            
            # Pivot the table
            wide_dataframe = dataframe.pivot_table(index="Time", columns="Parameter", values="Value")
            
            # Reindex to 49 steps (0 to 48 inclusive)
            wide_dataframe = wide_dataframe.reindex(range(49))
            if(imputed):
                wide_dataframe = wide_dataframe.ffill()
                wide_dataframe = wide_dataframe.fillna(-1)
            
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
        if(imputed):
            full_df=full_df.fillna(-1)


        print(full_df.head())
        if os.path.exists(output_path):
            if os.path.isdir(output_path):
                shutil.rmtree(output_path)  # Delete if it's a folder
            else:
                os.remove(output_path)      # Delete if it's a file

        # Save to parquet
        full_df.to_parquet(output_path, engine="pyarrow", index=False)