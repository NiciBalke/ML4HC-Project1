import torch
import pandas as pd
import numpy as np
from einops import rearrange
from tqdm import tqdm
import os
import csv
from chronos import ChronosPipeline

class ChannelTransformer:
    def __init__(self, n_channels=37, seq_len=48):
        self.n_channels = n_channels
        self.seq_len = seq_len

    def to_chronos_input(self, x: torch.Tensor) -> torch.Tensor:
        """
        x: (batch, seq_len, n_channels)
        returns: (batch * n_channels, seq_len) — one series per row
        """
        B, T, C = x.shape
        x_ci = rearrange(x, 'b t c -> (b c) t')
        return x_ci

    def normalize_per_channel(self, x: torch.Tensor) -> tuple[torch.Tensor, dict]:
        """
        x: (batch, seq_len, n_channels)
        """
        mean = x.mean(dim=1, keepdim=True)  # (B, 1, C)
        std  = x.std(dim=1, keepdim=True).clamp(min=1e-8)
        x_norm = (x - mean) / std
        return x_norm, {"mean": mean, "std": std}

    def patient_df_to_tensor(self, patient_df: pd.DataFrame, feature_cols: list) -> torch.Tensor:
        """
        Convert a single patient DataFrame to a (1, seq_len, n_channels) tensor.
        Pads or truncates to seq_len.
        """
        data = patient_df[feature_cols].values  # (T, C)

        # Pad or truncate to seq_len
        T, C = data.shape
        if T < self.seq_len:
            pad = np.zeros((self.seq_len - T, C))
            data = np.vstack([data, pad])
        else:
            data = data[:self.seq_len]

        # Fill NaNs with 0
        data = np.nan_to_num(data, nan=0.0)

        tensor = torch.tensor(data, dtype=torch.float32)  # (seq_len, n_channels)
        return tensor.unsqueeze(0)  # (1, seq_len, n_channels)


def extract_chronos_embedding(pipeline, series: torch.Tensor) -> np.ndarray:
    """
    series: (n_channels, seq_len) — output of to_chronos_input for one patient
    Returns: flattened embedding vector for the patient
    """
    embeddings = []

    # Chronos processes one series at a time
    for i in range(series.shape[0]):
        single = series[i].unsqueeze(0)  # (1, seq_len)

        with torch.no_grad():
            # encoder hidden states — last hidden layer
            embedding, _ = pipeline.embed(single)  # (1, seq_len, hidden_dim)
            # mean pool over time dimension → (1, hidden_dim)
            # pooled = embedding.mean(dim=1).squeeze(0).cpu().numpy()
            pooled = embedding[:, -1, :]   # last timestep instead of mean
            embeddings.append(pooled)

    # Concatenate all channel embeddings → one vector per patient
    return np.concatenate(embeddings)  # (n_channels * hidden_dim,)


########################################################################################
# Main
########################################################################################

full_df = pd.read_parquet("pData-b.parquet")

# Feature columns — drop non-feature cols
non_feature_cols = ["RecordID", "patient_id", "In-hospital_death"]
feature_cols = [c for c in full_df.columns if c not in non_feature_cols]
print(f"Features: {len(feature_cols)} — {feature_cols}")


device = "cuda" if torch.cuda.is_available() else "cpu"
print(f"Loading Chronos model on {device}...")

# Load Chronos
print("Loading Chronos model...")
pipeline = ChronosPipeline.from_pretrained(
    "amazon/chronos-t5-base",  # options: tiny, mini, small, base, large
    device_map= device,           # change to "cuda" if GPU available
    torch_dtype=torch.float32,
)

transformer = ChannelTransformer(n_channels=len(feature_cols), seq_len=48)

# Crash recovery
done_ids = set()
if os.path.exists("Embeddings/chronos_embeddings.csv"):
    done_ids = set(pd.read_csv("Embeddings/chronos_embeddings.csv", usecols=["RecordID"])["RecordID"].tolist())
print(f"Resuming: {len(done_ids)} patients already done")

for RecordID, patient_df in tqdm(full_df.groupby("RecordID")):
    if RecordID in done_ids:
        continue

    try:
        # 1. Convert to tensor (1, seq_len, n_channels)
        x = transformer.patient_df_to_tensor(patient_df, feature_cols)

        # 2. Normalize per channel
        x_norm, _ = transformer.normalize_per_channel(x)

        # 3. Flatten to (n_channels, seq_len) for Chronos
        x_ci = transformer.to_chronos_input(x_norm).squeeze(0)  # remove batch dim → (n_channels, seq_len)

        # 4. Extract embedding
        embedding = extract_chronos_embedding(pipeline, x_ci)

        # 5. Write to CSV immediately (crash recovery)
        if not os.path.exists("Embeddings/chronos_embeddings.csv"):
            with open("Embeddings/chronos_embeddings.csv", "w", newline="") as f:
                writer = csv.writer(f)
                header = ["RecordID"] + [f"emb_{i}" for i in range(len(embedding))]
                writer.writerow(header)

        with open("Embeddings/chronos_embeddings.csv", "a", newline="") as f:
            writer = csv.writer(f)
            writer.writerow([RecordID] + list(embedding))

    except Exception as e:
        print(f"Failed on RecordID {RecordID}: {e}")
        continue

print("Done! Loading for linear probe...")

# Load back for linear probe
embeddings_df = pd.read_csv("Embeddings/chronos_embeddings.csv").set_index("RecordID")
ground_truth  = pd.read_csv("processedOutcomes-b.txt").set_index("RecordID")

merged = ground_truth[["In-hospital_death"]].join(embeddings_df).dropna()

X = merged.drop(columns=["In-hospital_death"]).values
y = merged["In-hospital_death"].values

print(f"Final dataset: X={X.shape}, y={y.shape}")