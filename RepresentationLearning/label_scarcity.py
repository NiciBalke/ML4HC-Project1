import argparse
import importlib.util
import random
from pathlib import Path
from typing import Dict, List, Tuple

import numpy as np
import pandas as pd
import torch
import torch.nn as nn
import torch.optim as optim
from sklearn.impute import SimpleImputer
from torch.utils.data import DataLoader, TensorDataset


def _load_representation_learning_utils(repo_root: Path):
    module_path = repo_root / "RepresentationLearning" / "auto_encoder_base.py"
    spec = importlib.util.spec_from_file_location("RepresentationLearning_auto_encoder_base", module_path)
    if spec is None or spec.loader is None:
        raise ImportError(f"Could not load module from {module_path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class MyLSTM(nn.Module):
    """LSTM architecture aligned with LSTM_clean.py (input_dim parameterized)."""

    def __init__(self, input_dim: int, hidden_size: int = 64, num_layers: int = 2):
        super().__init__()
        self.lstm = nn.LSTM(
            input_size=input_dim,
            hidden_size=hidden_size,
            num_layers=num_layers,
            batch_first=True,
        )
        self.fc = nn.Sequential(
            nn.Linear(hidden_size, 32),
            nn.ReLU(),
            nn.Linear(32, 1),
        )
        self.sigmoid = nn.Sigmoid()

    def forward(self, x: torch.Tensor) -> torch.Tensor:
        out, _ = self.lstm(x)
        last_step_memory = out[:, -1, :]
        predi = self.fc(last_step_memory)
        return self.sigmoid(predi)


class SimpleTransformer(nn.Module):
    """Simple Transformer aligned with LSTM_clean.py."""

    def __init__(
        self,
        input_dim: int,
        d_model: int = 64,
        n_heads: int = 4,
        num_layers: int = 2,
        seq_len: int = 49,
    ):
        super().__init__()
        self.input_projection = nn.Linear(input_dim, d_model)
        self.pos_encoder = nn.Parameter(torch.randn(1, seq_len, d_model))

        encoder_layer = nn.TransformerEncoderLayer(
            d_model=d_model,
            nhead=n_heads,
            dim_feedforward=128,
            batch_first=True,
        )
        self.transformer_encoder = nn.TransformerEncoder(encoder_layer, num_layers=num_layers)
        self.fc = nn.Sequential(
            nn.Linear(d_model, 32),
            nn.ReLU(),
            nn.Linear(32, 1),
        )
        self.sigmoid = nn.Sigmoid()

    def forward(self, x: torch.Tensor) -> torch.Tensor:
        x = self.input_projection(x)
        x = x + self.pos_encoder
        x = self.transformer_encoder(x)
        x = x.mean(dim=1)
        predi = self.fc(x)
        return self.sigmoid(predi)


def set_seed(seed: int) -> None:
    random.seed(seed)
    np.random.seed(seed)
    torch.manual_seed(seed)
    if torch.cuda.is_available():
        torch.cuda.manual_seed(seed)
        torch.cuda.manual_seed_all(seed)


def prepare_patient_sequences(
    parquet_path: Path,
    train_idx: np.ndarray,
    seq_len: int = 49,
) -> Tuple[np.ndarray, np.ndarray, List[int], List[str]]:
    full_df = pd.read_parquet(parquet_path)

    # Keep consistency with pipeline
    if "ICUType" in full_df.columns:
        full_df = full_df.drop(columns=["ICUType"])

    patient_labels = (
        full_df.drop_duplicates(subset="PatientID")[["PatientID", "In-hospital_death"]]
        .sort_values("PatientID")
        .reset_index(drop=True)
    )
    patient_ids = patient_labels["PatientID"].tolist()
    labels = patient_labels["In-hospital_death"].to_numpy(dtype=np.int64)

    exclude_cols = ["PatientID", "In-hospital_death", "Time", "RecordID", "RecordID_x", "RecordID_y"]
    value_cols = [c for c in full_df.columns if c not in exclude_cols]

    # Forward-fill per patient
    full_df = full_df.sort_values(["PatientID", "Time"]).reset_index(drop=True)
    full_df[value_cols] = full_df.groupby("PatientID", sort=False)[value_cols].ffill()

    # Median imputer fit on train patients only
    train_patient_ids = set(np.array(patient_ids)[train_idx])
    train_rows = full_df[full_df["PatientID"].isin(train_patient_ids)]
    imputer = SimpleImputer(strategy="median")
    imputer.fit(train_rows[value_cols])
    full_df[value_cols] = imputer.transform(full_df[value_cols])

    # Build fixed-length sequences per patient in patient_labels order
    seqs = []
    for pid in patient_ids:
        group = full_df[full_df["PatientID"] == pid].sort_values("Time")
        arr = group[value_cols].to_numpy(dtype=np.float32)

        if arr.shape[0] < seq_len:
            pad = np.zeros((seq_len - arr.shape[0], arr.shape[1]), dtype=np.float32)
            arr = np.vstack([arr, pad])
        elif arr.shape[0] > seq_len:
            arr = arr[:seq_len]

        seqs.append(arr)

    X = np.stack(seqs, axis=0)
    return X, labels, patient_ids, value_cols


def train_nn_binary(
    model: nn.Module,
    x_train: np.ndarray,
    y_train: np.ndarray,
    device: torch.device,
    epochs: int,
    batch_size: int,
    lr: float,
    seed: int,
) -> nn.Module:
    model.to(device)
    model.train()

    x_tensor = torch.tensor(x_train, dtype=torch.float32)
    y_tensor = torch.tensor(y_train, dtype=torch.float32)
    dataset = TensorDataset(x_tensor, y_tensor)

    gen = torch.Generator()
    gen.manual_seed(seed)
    loader = DataLoader(dataset, batch_size=batch_size, shuffle=True, generator=gen)

    criterion = nn.BCELoss()
    optimizer = optim.Adam(model.parameters(), lr=lr)

    for _ in range(epochs):
        for xb, yb in loader:
            xb = xb.to(device)
            yb = yb.to(device)

            optimizer.zero_grad()
            preds = model(xb).squeeze(-1)
            loss = criterion(preds, yb)
            loss.backward()
            optimizer.step()

    return model


def predict_nn(model: nn.Module, x: np.ndarray, device: torch.device, batch_size: int = 256) -> np.ndarray:
    model.eval()
    probs = []
    with torch.no_grad():
        for i in range(0, len(x), batch_size):
            xb = torch.tensor(x[i : i + batch_size], dtype=torch.float32, device=device)
            p = model(xb).squeeze(-1).detach().cpu().numpy()
            probs.append(p)
    return np.concatenate(probs, axis=0)


def run_label_scarcity(parquet_path: Path, seed: int, epochs: int, batch_size: int, lr: float) -> pd.DataFrame:
    rl = _load_representation_learning_utils(Path(__file__).resolve().parents[1])
    print("[label_scarcity] Loading parquet and creating splits...", flush=True)

    # Use same split logic/metrics as RepresentationLearning auto_encoder_base.py
    tmp_df = pd.read_parquet(parquet_path)
    patient_labels = (
        tmp_df.drop_duplicates(subset="PatientID")[["PatientID", "In-hospital_death"]]
        .sort_values("PatientID")
        .reset_index(drop=True)
    )
    y_pat = patient_labels["In-hospital_death"].to_numpy(dtype=np.int64)

    train_idx, _, test_idx = rl._stratified_split_indices(y_pat, train_frac=0.7, val_frac=0.15, seed=seed)

    X_all, y_all, _, value_cols = prepare_patient_sequences(parquet_path, train_idx=train_idx)
    x_test = X_all[test_idx]
    y_test = y_all[test_idx]
    print(
        f"[label_scarcity] Prepared sequences: train={len(train_idx)}, test={len(test_idx)}, features={len(value_cols)}",
        flush=True,
    )

    device = torch.device("cuda" if torch.cuda.is_available() else "cpu")

    rows: List[Dict] = []
    for n in [100, 500, 1000]:
        print(f"[label_scarcity] Running train_size={n}...", flush=True)
        subset_idx = rl._stratified_subset_by_count(y_all, train_idx, n_samples=n, seed=seed)

        x_sub = X_all[subset_idx]
        y_sub = y_all[subset_idx]

        # 1) Random Forest from RepresentationLearning (flatten sequences)
        rf = rl.RandomForestModel()
        rf.train(x_sub.reshape(len(x_sub), -1), y_sub)
        rf_probs = rf.predict_proba(x_test.reshape(len(x_test), -1))
        rf_auroc, rf_auprc = rl.evaluate_metrics(y_test, rf_probs)
        print(
            f"[label_scarcity] train_size={n} random_forest: AUROC={rf_auroc:.4f}, AUPRC={rf_auprc:.4f}",
            flush=True,
        )

        # 2) LSTM from LSTM_clean architecture
        set_seed(seed)
        lstm = MyLSTM(input_dim=len(value_cols))
        lstm = train_nn_binary(lstm, x_sub, y_sub, device=device, epochs=epochs, batch_size=batch_size, lr=lr, seed=seed)
        lstm_probs = predict_nn(lstm, x_test, device=device)
        lstm_auroc, lstm_auprc = rl.evaluate_metrics(y_test, lstm_probs)
        print(
            f"[label_scarcity] train_size={n} lstm: AUROC={lstm_auroc:.4f}, AUPRC={lstm_auprc:.4f}",
            flush=True,
        )

        # 3) Simple Transformer from LSTM_clean architecture
        set_seed(seed)
        trf = SimpleTransformer(input_dim=len(value_cols), seq_len=x_sub.shape[1])
        trf = train_nn_binary(trf, x_sub, y_sub, device=device, epochs=epochs, batch_size=batch_size, lr=lr, seed=seed)
        trf_probs = predict_nn(trf, x_test, device=device)
        trf_auroc, trf_auprc = rl.evaluate_metrics(y_test, trf_probs)
        print(
            f"[label_scarcity] train_size={n} simple_transformer: AUROC={trf_auroc:.4f}, AUPRC={trf_auprc:.4f}",
            flush=True,
        )

        rows.extend(
            [
                {"train_size": n, "model": "random_forest", "auroc": rf_auroc, "auprc": rf_auprc},
                {"train_size": n, "model": "lstm", "auroc": lstm_auroc, "auprc": lstm_auprc},
                {"train_size": n, "model": "simple_transformer", "auroc": trf_auroc, "auprc": trf_auprc},
            ]
        )

    results = pd.DataFrame(rows)
    return results


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Label scarcity benchmark: RF vs LSTM vs Transformer")
    parser.add_argument("--parquet", type=str, default="processedDataProxy.parquet")
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--epochs", type=int, default=10)
    parser.add_argument("--batch-size", type=int, default=32)
    parser.add_argument("--lr", type=float, default=1e-3)
    parser.add_argument("--output-csv", type=str, default="RepresentationLearning/label_scarcity_results.csv")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    set_seed(args.seed)
    print("[label_scarcity] Starting benchmark...", flush=True)

    results = run_label_scarcity(
        parquet_path=Path(args.parquet),
        seed=args.seed,
        epochs=args.epochs,
        batch_size=args.batch_size,
        lr=args.lr,
    )

    print("\n=== Label Scarcity Results (Full Test Set) ===")
    print(results.sort_values(["train_size", "model"]).to_string(index=False))

    out_path = Path(args.output_csv)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    results.to_csv(out_path, index=False)
    print(f"\nSaved results to: {out_path}", flush=True)


if __name__ == "__main__":
    main()
