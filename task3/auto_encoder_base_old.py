import argparse
from dataclasses import dataclass
from typing import Dict, List, Tuple

import numpy as np
import pandas as pd
import torch
import torch.nn as nn
from torch.utils.data import DataLoader, Dataset
import matplotlib.pyplot as plt
import matplotlib
matplotlib.use("Agg")  # Use non-interactive backend

try:
    from sklearn.manifold import TSNE
    from sklearn.preprocessing import StandardScaler
    SKLEARN_AVAILABLE = True
except ImportError:
    SKLEARN_AVAILABLE = False

try:
    import umap
    UMAP_AVAILABLE = True
except ImportError:
    UMAP_AVAILABLE = False


PAD_TOKEN = "[PAD]"
UNK_TOKEN = "[UNK]"
BOS_TOKEN = "[BOS]"
EOS_TOKEN = "[EOS]"


@dataclass
class TokenizedCohort:
    input_ids: torch.Tensor
    attention_mask: torch.Tensor
    labels: torch.Tensor
    patient_ids: List[int]
    vocab: Dict[str, int]


class PatientTokenDataset(Dataset):
    def __init__(self, input_ids: torch.Tensor, attention_mask: torch.Tensor, labels: torch.Tensor):
        self.input_ids = input_ids
        self.attention_mask = attention_mask
        self.labels = labels

    def __len__(self) -> int:
        return self.input_ids.shape[0]

    def __getitem__(self, idx: int):
        return {
            "input_ids": self.input_ids[idx],
            "attention_mask": self.attention_mask[idx],
            "label": self.labels[idx],
        }


def _binary_auroc(y_true: np.ndarray, y_score: np.ndarray) -> float:
    """
    AUROC for binary labels without external dependencies.
    Uses average ranks for ties.
    """
    y_true = np.asarray(y_true).astype(int)
    y_score = np.asarray(y_score).astype(float)

    pos = (y_true == 1)
    neg = (y_true == 0)
    n_pos = int(pos.sum())
    n_neg = int(neg.sum())

    if n_pos == 0 or n_neg == 0:
        return float("nan")

    order = np.argsort(y_score)
    sorted_scores = y_score[order]
    ranks = np.empty_like(sorted_scores, dtype=float)

    i = 0
    n = len(sorted_scores)
    while i < n:
        j = i
        while j + 1 < n and sorted_scores[j + 1] == sorted_scores[i]:
            j += 1
        avg_rank = (i + j + 2) / 2.0  # 1-based average rank
        ranks[i : j + 1] = avg_rank
        i = j + 1

    original_ranks = np.empty_like(ranks)
    original_ranks[order] = ranks
    sum_pos_ranks = original_ranks[pos].sum()
    auc = (sum_pos_ranks - n_pos * (n_pos + 1) / 2.0) / (n_pos * n_neg)
    return float(auc)


def _binary_auprc(y_true: np.ndarray, y_score: np.ndarray) -> float:
    """
    AUPRC (Area Under the Precision-Recall Curve) for binary labels.
    Computes precision and recall at each unique threshold.
    """
    y_true = np.asarray(y_true).astype(int)
    y_score = np.asarray(y_score).astype(float)

    n_pos = int((y_true == 1).sum())
    n_neg = int((y_true == 0).sum())

    if n_pos == 0 or n_neg == 0:
        return float("nan")

    order = np.argsort(-y_score)  # descending by score
    sorted_labels = y_true[order]

    tp = np.cumsum(sorted_labels)
    fp = np.cumsum(1 - sorted_labels)

    # Precision = TP / (TP + FP)
    # Recall = TP / n_pos
    precision = tp / (tp + fp)
    recall = tp / n_pos

    # Add boundary: (recall=0, precision=1) at threshold = +inf
    recall = np.concatenate(([0], recall))
    precision = np.concatenate(([1], precision))

    # Trapezoidal rule (manual integration)
    auprc = 0.0
    for i in range(1, len(recall)):
        width = recall[i] - recall[i - 1]
        height = (precision[i] + precision[i - 1]) / 2.0
        auprc += width * height
    return float(auprc)


def _stratified_split_indices(
    labels: torch.Tensor,
    train_frac: float,
    val_frac: float,
    seed: int,
) -> Tuple[np.ndarray, np.ndarray, np.ndarray]:
    if train_frac <= 0 or val_frac < 0 or (train_frac + val_frac) >= 1:
        raise ValueError("Require train_frac > 0, val_frac >= 0 and train_frac + val_frac < 1.")

    rng = np.random.default_rng(seed)
    y = labels.cpu().numpy().astype(int)

    train_idx: List[int] = []
    val_idx: List[int] = []
    test_idx: List[int] = []

    for cls in np.unique(y):
        cls_idx = np.where(y == cls)[0]
        rng.shuffle(cls_idx)

        n = len(cls_idx)
        n_train = int(round(n * train_frac))
        n_val = int(round(n * val_frac))
        n_train = min(n_train, n)
        n_val = min(n_val, n - n_train)

        train_idx.extend(cls_idx[:n_train].tolist())
        val_idx.extend(cls_idx[n_train : n_train + n_val].tolist())
        test_idx.extend(cls_idx[n_train + n_val :].tolist())

    train_idx = np.array(train_idx, dtype=int)
    val_idx = np.array(val_idx, dtype=int)
    test_idx = np.array(test_idx, dtype=int)

    rng.shuffle(train_idx)
    rng.shuffle(val_idx)
    rng.shuffle(test_idx)
    return train_idx, val_idx, test_idx


def _stratified_subset_by_count(
    labels: torch.Tensor,
    indices: np.ndarray,
    n_samples: int,
    seed: int,
) -> np.ndarray:
    """
    Stratified sample n_samples from indices, preserving label distribution.
    """
    rng = np.random.default_rng(seed)
    y_full = labels.cpu().numpy().astype(int)
    y_subset = y_full[indices]

    subset_indices: List[int] = []
    for cls in np.unique(y_subset):
        cls_mask = y_subset == cls
        cls_locs = np.where(cls_mask)[0]
        n_cls = max(1, int(round(n_samples * (cls_mask.sum() / len(y_subset)))))
        n_cls = min(n_cls, len(cls_locs))
        sampled = rng.choice(cls_locs, size=n_cls, replace=False)
        subset_indices.extend(sampled.tolist())

    subset_indices = np.array(subset_indices, dtype=int)
    rng.shuffle(subset_indices)
    return indices[subset_indices]


class SequenceAutoEncoder(nn.Module):
    """Token sequence autoencoder with reconstruction loss over tuple tokens."""

    def __init__(
        self,
        vocab_size: int,
        d_model: int = 128,
        nhead: int = 4,
        num_layers: int = 2,
        max_len: int = 2048,
        dropout: float = 0.1,
    ):
        super().__init__()
        self.token_embedding = nn.Embedding(vocab_size, d_model)
        self.position_embedding = nn.Embedding(max_len, d_model)

        encoder_layer = nn.TransformerEncoderLayer(
            d_model=d_model,
            nhead=nhead,
            dim_feedforward=d_model * 4,
            dropout=dropout,
            batch_first=True,
            activation="gelu",
        )
        self.encoder = nn.TransformerEncoder(encoder_layer, num_layers=num_layers)
        self.norm = nn.LayerNorm(d_model)
        self.output_head = nn.Linear(d_model, vocab_size)

    def encode(self, input_ids: torch.Tensor, attention_mask: torch.Tensor) -> torch.Tensor:
        batch_size, seq_len = input_ids.shape
        positions = torch.arange(seq_len, device=input_ids.device).unsqueeze(0).expand(batch_size, seq_len)
        x = self.token_embedding(input_ids) + self.position_embedding(positions)

        key_padding_mask = attention_mask == 0
        hidden = self.encoder(x, src_key_padding_mask=key_padding_mask)
        hidden = self.norm(hidden)

        mask = attention_mask.unsqueeze(-1).float()
        pooled = (hidden * mask).sum(dim=1) / mask.sum(dim=1).clamp(min=1.0)
        return pooled

    def forward(self, input_ids: torch.Tensor, attention_mask: torch.Tensor) -> Tuple[torch.Tensor, torch.Tensor]:
        batch_size, seq_len = input_ids.shape
        positions = torch.arange(seq_len, device=input_ids.device).unsqueeze(0).expand(batch_size, seq_len)
        x = self.token_embedding(input_ids) + self.position_embedding(positions)

        key_padding_mask = attention_mask == 0
        hidden = self.encoder(x, src_key_padding_mask=key_padding_mask)
        hidden = self.norm(hidden)
        logits = self.output_head(hidden)

        mask = attention_mask.unsqueeze(-1).float()
        pooled = (hidden * mask).sum(dim=1) / mask.sum(dim=1).clamp(min=1.0)
        return logits, pooled


class LinearProbe(nn.Module):
    def __init__(self, latent_dim: int, num_classes: int = 2):
        super().__init__()
        self.linear = nn.Linear(latent_dim, num_classes)

    def forward(self, z: torch.Tensor) -> torch.Tensor:
        return self.linear(z)


def _value_to_bin_codes(values: pd.Series, n_bins: int) -> pd.Series:
    """
    Quantile-bins values within one parameter channel.
    Falls back to a single bin if qcut cannot create bins (e.g. constant values).
    """
    try:
        return pd.qcut(values, q=n_bins, labels=False, duplicates="drop")
    except ValueError:
        return pd.Series(np.zeros(len(values), dtype=int), index=values.index)


def build_tuple_token_dataset(
    parquet_path: str,
    n_value_bins: int = 10,
    max_seq_len: int = 1024,
) -> TokenizedCohort:
    full_df = pd.read_parquet(parquet_path)

    if "PatientID" not in full_df.columns or "Time" not in full_df.columns:
        raise ValueError("Expected columns `PatientID` and `Time` in parquet file.")

    if "In-hospital_death" not in full_df.columns:
        raise ValueError("Expected target column `In-hospital_death` in parquet file.")

    id_cols = ["PatientID", "Time", "In-hospital_death"]
    # Handle merge artifacts from preprocessing script.
    extra_id_cols = [c for c in full_df.columns if c.startswith("RecordID")]
    id_cols.extend(extra_id_cols)

    measurement_cols = [c for c in full_df.columns if c not in id_cols]

    long_df = full_df.melt(
        id_vars=["PatientID", "Time", "In-hospital_death"],
        value_vars=measurement_cols,
        var_name="Parameter",
        value_name="Value",
    )
    long_df = long_df.dropna(subset=["Value"])  # only observed events become tokens

    long_df["time_bin"] = long_df["Time"].astype(int).clip(0, 48)

    long_df["value_bin"] = long_df.groupby("Parameter")["Value"].transform(
        lambda s: _value_to_bin_codes(s, n_bins=n_value_bins)
    )
    long_df = long_df.dropna(subset=["value_bin"])
    long_df["value_bin"] = long_df["value_bin"].astype(int)

    long_df["tuple_token"] = (
        long_df["Parameter"].astype(str)
        + "|t"
        + long_df["time_bin"].astype(str)
        + "|v"
        + long_df["value_bin"].astype(str)
    )

    base_vocab = [PAD_TOKEN, UNK_TOKEN, BOS_TOKEN, EOS_TOKEN]
    observed_tokens = sorted(long_df["tuple_token"].unique().tolist())
    vocab = {token: idx for idx, token in enumerate(base_vocab + observed_tokens)}
    long_df["token_id"] = long_df["tuple_token"].map(vocab).fillna(vocab[UNK_TOKEN]).astype(int)

    labels_by_patient = full_df.groupby("PatientID")["In-hospital_death"].first().astype(int)

    seqs = (
        long_df.sort_values(["PatientID", "time_bin", "Parameter"])
        .groupby("PatientID")["token_id"]
        .apply(list)
    )

    patient_ids = sorted(seqs.index.tolist())
    bos_id = vocab[BOS_TOKEN]
    eos_id = vocab[EOS_TOKEN]
    pad_id = vocab[PAD_TOKEN]

    token_sequences: List[List[int]] = []
    label_list: List[int] = []

    for pid in patient_ids:
        seq = [bos_id] + seqs.loc[pid] + [eos_id]
        seq = seq[:max_seq_len]
        token_sequences.append(seq)
        label_list.append(int(labels_by_patient.loc[pid]))

    if not token_sequences:
        raise ValueError("No token sequences were created. Check input preprocessing and missing values.")

    seq_len = max(len(seq) for seq in token_sequences)
    input_ids = np.full((len(token_sequences), seq_len), pad_id, dtype=np.int64)
    attention_mask = np.zeros((len(token_sequences), seq_len), dtype=np.int64)

    for i, seq in enumerate(token_sequences):
        input_ids[i, : len(seq)] = seq
        attention_mask[i, : len(seq)] = 1

    return TokenizedCohort(
        input_ids=torch.tensor(input_ids, dtype=torch.long),
        attention_mask=torch.tensor(attention_mask, dtype=torch.long),
        labels=torch.tensor(label_list, dtype=torch.long),
        patient_ids=patient_ids,
        vocab=vocab,
    )


def train_autoencoder(
    model: SequenceAutoEncoder,
    dataloader: DataLoader,
    pad_token_id: int,
    device: torch.device,
    epochs: int = 5,
    lr: float = 1e-3,
) -> None:
    model.to(device)
    optimizer = torch.optim.AdamW(model.parameters(), lr=lr)
    criterion = nn.CrossEntropyLoss(ignore_index=pad_token_id)

    for epoch in range(1, epochs + 1):
        model.train()
        running_loss = 0.0
        n_batches = 0

        for batch in dataloader:
            input_ids = batch["input_ids"].to(device)
            attention_mask = batch["attention_mask"].to(device)

            logits, _ = model(input_ids, attention_mask)
            # Reconstruction target = original token IDs
            loss = criterion(logits.view(-1, logits.size(-1)), input_ids.view(-1))

            optimizer.zero_grad()
            loss.backward()
            optimizer.step()

            running_loss += loss.item()
            n_batches += 1

        print(f"[AE] epoch={epoch} loss={running_loss / max(n_batches, 1):.4f}")


def train_linear_probe(
    encoder: SequenceAutoEncoder,
    train_dataloader: DataLoader,
    val_dataloader: DataLoader,
    latent_dim: int,
    device: torch.device,
    epochs: int = 10,
    lr: float = 1e-3,
) -> LinearProbe:
    encoder.eval()
    for param in encoder.parameters():
        param.requires_grad = False

    probe = LinearProbe(latent_dim=latent_dim, num_classes=2).to(device)
    optimizer = torch.optim.AdamW(probe.parameters(), lr=lr)
    criterion = nn.CrossEntropyLoss()
    best_val_auroc = -np.inf
    best_state = None

    for epoch in range(1, epochs + 1):
        probe.train()
        running_loss = 0.0
        n_batches = 0

        for batch in train_dataloader:
            input_ids = batch["input_ids"].to(device)
            attention_mask = batch["attention_mask"].to(device)
            labels = batch["label"].to(device)

            with torch.no_grad():
                z = encoder.encode(input_ids, attention_mask)

            logits = probe(z)
            loss = criterion(logits, labels)

            optimizer.zero_grad()
            loss.backward()
            optimizer.step()

            running_loss += loss.item()
            n_batches += 1

        val_auroc, val_auprc = evaluate_probe_metrics(encoder, probe, val_dataloader, device)
        if np.isfinite(val_auroc) and val_auroc > best_val_auroc:
            best_val_auroc = val_auroc
            best_state = {k: v.detach().cpu().clone() for k, v in probe.state_dict().items()}

        print(
            f"[Probe] epoch={epoch} train_loss={running_loss / max(n_batches, 1):.4f} "
            f"val_auroc={val_auroc:.4f} val_auprc={val_auprc:.4f}"
        )

    if best_state is not None:
        probe.load_state_dict(best_state)

    return probe


@torch.no_grad()
def evaluate_probe_metrics(
    encoder: SequenceAutoEncoder,
    probe: LinearProbe,
    dataloader: DataLoader,
    device: torch.device,
) -> Tuple[float, float]:
    """
    Evaluate probe on a dataloader. Returns (AUROC, AUPRC).
    """
    encoder.eval()
    probe.eval()

    all_probs: List[np.ndarray] = []
    all_labels: List[np.ndarray] = []

    for batch in dataloader:
        input_ids = batch["input_ids"].to(device)
        attention_mask = batch["attention_mask"].to(device)
        labels = batch["label"].cpu().numpy()

        z = encoder.encode(input_ids, attention_mask)
        logits = probe(z)
        probs = torch.softmax(logits, dim=1)[:, 1].cpu().numpy()

        all_probs.append(probs)
        all_labels.append(labels)

    if not all_probs:
        return float("nan"), float("nan")

    y_score = np.concatenate(all_probs)
    y_true = np.concatenate(all_labels)
    auroc = _binary_auroc(y_true=y_true, y_score=y_score)
    auprc = _binary_auprc(y_true=y_true, y_score=y_score)
    return auroc, auprc


def get_embeddings(model: SequenceAutoEncoder, dataloader: DataLoader, device: torch.device) -> tuple[np.ndarray, np.ndarray]:
    """Extract embeddings and labels from a dataloader.
    
    Args:
        model: Trained autoencoder model
        dataloader: DataLoader with dicts containing 'input_ids', 'attention_mask', and 'label'
        device: Device to run on
    
    Returns:
        Tuple of (embeddings: [N, d_model], labels: [N])
    """
    model.eval()
    embeddings_list = []
    labels_list = []
    
    with torch.no_grad():
        for batch in dataloader:
            token_ids = batch["input_ids"].to(device)
            attention_mask = batch["attention_mask"].to(device)
            labels = batch["label"].to(device)
            
            # Get embeddings from encoder (masked mean pooling)
            embeds = model.encode(token_ids, attention_mask)
            embeddings_list.append(embeds.cpu().numpy())
            labels_list.append(labels.cpu().numpy())
    
    embeddings = np.concatenate(embeddings_list, axis=0)
    labels = np.concatenate(labels_list, axis=0)
    return embeddings, labels


def visualize_embeddings(model: SequenceAutoEncoder, train_loader: DataLoader, device: torch.device, output_dir: str = "./embeddings"):
    """Visualize learned embeddings using t-SNE and UMAP.
    
    Args:
        model: Trained autoencoder model
        train_loader: DataLoader for training set
        device: Device to run on
        output_dir: Directory to save plots
    """
    import os
    os.makedirs(output_dir, exist_ok=True)
    
    # Extract embeddings
    embeddings, labels = get_embeddings(model, train_loader, device)
    
    # Standardize embeddings for better visualization
    if not SKLEARN_AVAILABLE:
        print("sklearn not available; standardizing manually")
        embeddings = (embeddings - embeddings.mean(axis=0)) / (embeddings.std(axis=0) + 1e-8)
    else:
        scaler = StandardScaler()
        embeddings = scaler.fit_transform(embeddings)
    
    # Color mapping: 0 (survived) = blue, 1 (death) = orange
    colors = ['blue' if label == 0 else 'orange' for label in labels]
    
    # t-SNE visualization
    if SKLEARN_AVAILABLE:
        try:
            print("Computing t-SNE...")
            tsne = TSNE(n_components=2, random_state=42, max_iter=1000, perplexity=30)
            embeddings_tsne = tsne.fit_transform(embeddings)
            
            plt.figure(figsize=(10, 8))
            scatter = plt.scatter(embeddings_tsne[:, 0], embeddings_tsne[:, 1], c=colors, alpha=0.6, s=50)
            plt.xlabel("t-SNE 1")
            plt.ylabel("t-SNE 2")
            plt.title("Autoencoder Embeddings - t-SNE (Blue=Survived, Orange=Death)")
            plt.tight_layout()
            tsne_path = os.path.join(output_dir, "tsne_embeddings.png")
            plt.savefig(tsne_path, dpi=150, bbox_inches='tight')
            plt.close()
            print(f"Saved t-SNE plot to {tsne_path}")
        except Exception as e:
            print(f"t-SNE visualization failed: {e}")
    else:
        print("sklearn not available; skipping t-SNE visualization")
    
    # UMAP visualization (if available)
    if UMAP_AVAILABLE:
        try:
            print("Computing UMAP...")
            reducer = umap.UMAP(n_components=2, random_state=42, n_neighbors=15, min_dist=0.1)
            embeddings_umap = reducer.fit_transform(embeddings)
            
            plt.figure(figsize=(10, 8))
            scatter = plt.scatter(embeddings_umap[:, 0], embeddings_umap[:, 1], c=colors, alpha=0.6, s=50)
            plt.xlabel("UMAP 1")
            plt.ylabel("UMAP 2")
            plt.title("Autoencoder Embeddings - UMAP (Blue=Survived, Orange=Death)")
            plt.tight_layout()
            umap_path = os.path.join(output_dir, "umap_embeddings.png")
            plt.savefig(umap_path, dpi=150, bbox_inches='tight')
            plt.close()
            print(f"Saved UMAP plot to {umap_path}")
        except Exception as e:
            print(f"UMAP visualization failed: {e}")
    else:
        print("UMAP not installed; skipping UMAP visualization. Install with: pip install umap-learn")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Tuple-token sequence autoencoder baseline")
    parser.add_argument("--parquet", type=str, default="processedDataProxy.parquet")
    parser.add_argument("--batch-size", type=int, default=64)
    parser.add_argument("--max-seq-len", type=int, default=1024)
    parser.add_argument("--value-bins", type=int, default=10)
    parser.add_argument("--d-model", type=int, default=128)
    parser.add_argument("--nhead", type=int, default=4)
    parser.add_argument("--layers", type=int, default=2)
    parser.add_argument("--ae-epochs", type=int, default=5)
    parser.add_argument("--probe-epochs", type=int, default=10)
    parser.add_argument("--lr", type=float, default=1e-3)
    parser.add_argument("--train-frac", type=float, default=0.7)
    parser.add_argument("--val-frac", type=float, default=0.15)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--output-dir", type=str, default="./embeddings")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    # MPS has incomplete support for some Transformer ops used here.
    # Prefer CUDA when available, otherwise use CPU for stable execution.
    device = torch.device("cuda") if torch.cuda.is_available() else torch.device("cpu")
    print(f"Using device: {device}")

    cohort = build_tuple_token_dataset(
        parquet_path=args.parquet,
        n_value_bins=args.value_bins,
        max_seq_len=args.max_seq_len,
    )
    print(
        f"Built tokenized cohort: patients={cohort.input_ids.shape[0]}, "
        f"seq_len={cohort.input_ids.shape[1]}, vocab={len(cohort.vocab)}"
    )

    train_idx, val_idx, test_idx = _stratified_split_indices(
        labels=cohort.labels,
        train_frac=args.train_frac,
        val_frac=args.val_frac,
        seed=args.seed,
    )

    print(f"Data split -> train={len(train_idx)}, val={len(val_idx)}, test={len(test_idx)}")

    # Create datasets
    train_dataset = PatientTokenDataset(
        cohort.input_ids[train_idx],
        cohort.attention_mask[train_idx],
        cohort.labels[train_idx],
    )
    val_dataset = PatientTokenDataset(
        cohort.input_ids[val_idx],
        cohort.attention_mask[val_idx],
        cohort.labels[val_idx],
    )
    test_dataset = PatientTokenDataset(
        cohort.input_ids[test_idx],
        cohort.attention_mask[test_idx],
        cohort.labels[test_idx],
    )

    train_loader = DataLoader(train_dataset, batch_size=args.batch_size, shuffle=True)
    val_loader = DataLoader(val_dataset, batch_size=args.batch_size, shuffle=False)
    test_loader = DataLoader(test_dataset, batch_size=args.batch_size, shuffle=False)

    print(f"\n{'='*60}")
    print("Training autoencoder on full dataset")
    print(f"{'='*60}")

    model = SequenceAutoEncoder(
        vocab_size=len(cohort.vocab),
        d_model=args.d_model,
        nhead=args.nhead,
        num_layers=args.layers,
        max_len=cohort.input_ids.shape[1],
    )

    train_autoencoder(
        model=model,
        dataloader=train_loader,
        pad_token_id=cohort.vocab[PAD_TOKEN],
        device=device,
        epochs=args.ae_epochs,
        lr=args.lr,
    )

    probe = train_linear_probe(
        encoder=model,
        train_dataloader=train_loader,
        val_dataloader=val_loader,
        latent_dim=args.d_model,
        device=device,
        epochs=args.probe_epochs,
        lr=args.lr,
    )

    test_auroc, test_auprc = evaluate_probe_metrics(model, probe, test_loader, device)
    print(f"\n{'='*60}")
    print("Test set metrics:")
    print(f"{'='*60}")
    print(f"AUROC: {test_auroc:.4f}")
    print(f"AUPRC: {test_auprc:.4f}")

    print(f"\n{'='*60}")
    print("Visualizing learned embeddings")
    print(f"{'='*60}")
    visualize_embeddings(model, train_loader, device, output_dir=args.output_dir)



if __name__ == "__main__":
    main()