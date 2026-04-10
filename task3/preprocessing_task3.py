import os
import shutil

import pandas as pd
from tqdm import tqdm

pathToData = "ml4h_data/p1/set-a"
outcome_path = "ml4h_data/p1/Outcomes-a.txt"

all_data = []

# Load outcomes and keep target label only
outcomes_df = pd.read_csv(outcome_path)
outcomes_df = outcomes_df[["RecordID", "In-hospital_death"]]

for file in tqdm(os.listdir(pathToData)):
    filepath = os.path.join(pathToData, file)
    if not filepath.endswith(".txt"):
        continue

    record_id = int(file.replace(".txt", ""))
    dataframe = pd.read_csv(filepath)

    # Round time UP to preserve causality
    dataframe["Time"] = dataframe["Time"].apply(
        lambda x: int(x[:2]) if (x[3:] == "00") else (1 + int(x[:2]))
    )

    # Pivot to wide format, keep 49 time steps (0..48)
    wide_dataframe = dataframe.pivot_table(index="Time", columns="Parameter", values="Value")
    wide_dataframe = wide_dataframe.reindex(range(49))
    wide_dataframe["PatientID"] = record_id
    wide_dataframe = wide_dataframe.reset_index()
    all_data.append(wide_dataframe)

full_df = pd.concat(all_data, ignore_index=True)
full_df = full_df.merge(outcomes_df, left_on="PatientID", right_on="RecordID", how="left")

# Drop ICUType here as requested for task3 preprocessing
if "ICUType" in full_df.columns:
    full_df = full_df.drop(columns=["ICUType"])

output_path = "processedDataProxy.parquet"
print(full_df.head())
if os.path.exists(output_path):
    if os.path.isdir(output_path):
        shutil.rmtree(output_path)
    else:
        os.remove(output_path)

# NOTE: Imputation and scaling are applied in task3/auto_encoder_base.py
full_df.to_parquet(output_path, engine="pyarrow", index=False)