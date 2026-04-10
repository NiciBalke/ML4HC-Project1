import torch
import ollama as lama
import pandas as pd
import re
import sklearn.metrics as metrics
from pathlib import Path
from tqdm import tqdm
import numpy as np
import os
import csv
from sklearn.metrics import (
    roc_auc_score,
    brier_score_loss,
    log_loss,
    classification_report,
    average_precision_score
)

### Env path
## /home/nbalke/jupyter/bin


# lama start command: 
# OLLAMA_MODELS=/cluster/courses/ml4h/llm/models /cluster/courses/ml4h/llm/bin/ollama serve

def extract_score(response: str) -> float | None:
    response = response.strip()
    try:
        val = float(response)
        if 0.0 <= val <= 1.0:
            return val
    except ValueError:
        pass
    matches = re.findall(r'\b(0\.\d+|1\.0+|0|1)\b', response)
    valid = [float(m) for m in matches if 0.0 <= float(m) <= 1.0]
    if len(valid) >= 1:
        return valid[0]
    return -1

def extract_summary(patient_df: pd.DataFrame, outcome: int = None) -> str:
    """Extract a compact clinical summary from a patient's 48h data."""
    
    age         = patient_df["Age"].iloc[0]
    gcs_min     = patient_df["GCS"].min()
    
    # MAP features
    map_series      = patient_df["MAP"].dropna()
    map_min         = map_series.min()
    map_low_frac    = (map_series < 65).mean().round(2)
    map_low_hours   = int((map_series < 65).sum())
    
    # MAP trend: compare first half vs second half mean
    mid = len(map_series) // 2
    map_first_half  = map_series.iloc[:mid].mean()
    map_second_half = map_series.iloc[mid:].mean()
    diff = map_second_half - map_first_half
    if diff > 5:
        map_trend = "improving"
    elif diff < -5:
        map_trend = "worsening"
    else:
        map_trend = "stable"

    # HR
    hr_min      = patient_df["HR"].min()
    hr_max      = patient_df["HR"].max()
    
    # Temp
    temp_max    = patient_df["Temp"].max()
    
    # Urine
    urine_series    = patient_df["Urine"].dropna()
    urine_total     = urine_series.sum()
    urine_min       = urine_series.min()
    urine_low_hours = int((urine_series < 30).sum())  # < 30ml/hr is oliguria threshold
    if urine_total > 1500:
        urine_label = "high"
    elif urine_total > 500:
        urine_label = "normal"
    else:
        urine_label = "low"

    # Labs
    creat_max   = patient_df["Creatinine"].max()
    wbc_max     = patient_df["WBC"].max()
    glucose_max = patient_df["Glucose"].max()
    
    # Respiratory
    resp_max    = patient_df["RespRate"].max()

    summary = (
        f"Age: {age:.0f}\n"
        f"GCS_min (neurological score): {gcs_min:.0f}\n"
        f"MAP_min (mean arterial pressure): {map_min:.1f}\n"
        f"MAP_low_fraction: {map_low_frac}\n"
        f"MAP_low_hours: {map_low_hours}\n"
        f"MAP_trend: {map_trend}\n"
        f"HR_range: {hr_min:.0f}-{hr_max:.0f}\n"
        f"Temp_max: {temp_max:.1f}\n"
        f"Urine_total: {urine_label}\n"
        f"Urine_min: {urine_min:.0f}\n"
        f"Urine_low_hours: {urine_low_hours}\n"
        f"Creatinine_max: {creat_max:.1f}\n"
        f"WBC_max: {wbc_max:.1f}\n"
        f"Glucose_max: {glucose_max:.0f}\n"
        f"RespRate_max: {resp_max:.0f}\n"
    )
    
    return summary

########################################################################################
# Parameters
########################################################################################

# MODES
predict_mode = 1
embed_mode = 0





full_df = pd.read_parquet("pData-b.parquet")
examples_df = pd.read_parquet("processedData-a.parquet")


print("prompting to ollama")

with open("Prompt.md", "r") as f:
    template = f.read()

# ✅ Load examples once outside the loop
data_streamEx1 = extract_summary(examples_df[examples_df["RecordID"] == 132539]) ## 0
data_streamEx2 = extract_summary(examples_df[examples_df["RecordID"] == 132551]) ## 1
data_streamEx3 = extract_summary(examples_df[examples_df["RecordID"] == 132540]) ## 0
data_streamEx4 = extract_summary(examples_df[examples_df["RecordID"] == 132588]) ## 1
data_streamEx5 = extract_summary(examples_df[examples_df["RecordID"] == 132602]) ## 1
data_streamEx6 = extract_summary(examples_df[examples_df["RecordID"] == 132686]) ## 0
data_streamEx7 = extract_summary(examples_df[examples_df["RecordID"] == 132614]) ## 0

# ✅ Create files with headers if they don't exist, load already done IDs for crash recovery
if predict_mode and not embed_mode:
    if not os.path.exists("predictions.csv"):
        with open("predictions.csv", "w") as f:
            f.write("RecordID,predicted\n")
    done_ids = set(pd.read_csv("predictions.csv")["RecordID"].tolist()) if os.path.getsize("predictions.csv") > 20 else set()
    print(f"Resuming: {len(done_ids)} predictions already done")

if predict_mode and embed_mode:
    # For embeddings we need to know the embedding size first, so we init the file lazily
    done_ids = set()
    if os.path.exists("embeddings.csv"):
        done_ids = set(pd.read_csv("embeddings.csv", usecols=["RecordID"])["RecordID"].tolist())
    print(f"Resuming: {len(done_ids)} embeddings already done")

if predict_mode:
    for RecordID, patient_df in tqdm(full_df.groupby("RecordID")):

        if RecordID in done_ids:
            continue  # ✅ skip already processed

        path_to_data = patient_df.to_csv(index=False)

        prompt = template.format(
            ex1= data_streamEx1,
            ex2= data_streamEx2,
            ex3= data_streamEx3,
            ex4= data_streamEx4,
            ex5= data_streamEx5,
            ex6= data_streamEx4,
            ex7= data_streamEx5,
            data=extract_summary(patient_df)
        )

        if not embed_mode:
            # ✅ Prediction mode
            stream = lama.chat(
                model= 'gemma3:1b',  # 'gemma3:1b',
                messages=[{'role': 'user', 'content': prompt}],
                stream=True,
                options={"num_ctx": 8192} 
            )
            full_response = ""
            for chunk in stream:
                full_response += chunk['message']['content']

            score = extract_score(full_response)

            # ✅ Write prediction immediately
            with open("predictions.csv", "a") as f:
                f.write(f"{RecordID},{score}\n")

        else:
            # ✅ Embedding mode
            response = lama.embed(
                model='nomic-embed-text:latest',
                input=prompt,
                options={"num_ctx": 16384} 
            )
            embedding = response['embeddings'][0]

            # ✅ Write embedding immediately — init file with header on first write
            if not os.path.exists("embeddings.csv"):
                with open("embeddings.csv", "w", newline="") as f:
                    writer = csv.writer(f)
                    header = ["RecordID"] + [f"emb_{i}" for i in range(len(embedding))]
                    writer.writerow(header)

            with open("embeddings.csv", "a", newline="") as f:
                writer = csv.writer(f)
                writer.writerow([RecordID] + list(embedding))


########################################################################################
# Metrics
########################################################################################

ground_truth = pd.read_csv("processedOutcomes-b.txt")
ground_truth = ground_truth.set_index("RecordID")

if not predict_mode:
    output_df = pd.read_csv("FINAL_predictions.csv")
    predictions = output_df.set_index("RecordID")["predicted"]
else:
    if not embed_mode:
        predictions = pd.read_csv("predictions.csv").set_index("RecordID")["predicted"]
    else:
        print("Embed mode — no predictions to score")
        exit()

failed = predictions[predictions == -1]
valid  = predictions[predictions != -1]

print(f"Valid:  {len(valid)} / {len(predictions)}")
print(f"Failed: {len(failed)} / {len(predictions)}")

merged = ground_truth[["In-hospital_death"]].join(valid)
merged = merged.dropna()

y_true = merged["In-hospital_death"].values
y_prob = merged["predicted"].values

auroc     = roc_auc_score(y_true, y_prob)
brier   = brier_score_loss(y_true, y_prob)
logloss = log_loss(y_true, y_prob)
auprc = average_precision_score(y_true, y_prob)

print(f"\nMetrics on {len(merged)} valid predictions:")
print(f"AUROC:         {auroc:.3f}")
print(f"Brier Score: {brier:.3f}")
print(f"Log Loss:    {logloss:.3f}")
print(f"AUPRC:         {auprc:.3f}")