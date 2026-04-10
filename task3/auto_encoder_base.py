"""
Q3: Representation Learning for Medical Time Series

Q3.1 - Pretraining and Linear Probes: Self-supervised encoder pretraining with linear probe evaluation
Q3.2 - Label Scarcity: Supervised vs pretrained models under limited labeling
Q3.3 - Visualization: t-SNE/UMAP visualization and clustering metrics
"""

import argparse
import json
import os
import random
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

# Make CUDA GEMM deterministic when CUDA is available.
os.environ.setdefault("CUBLAS_WORKSPACE_CONFIG", ":4096:8")

import torch
import torch.nn as nn
import torch.nn.functional as F
from torch.utils.data import DataLoader, Dataset
import matplotlib.pyplot as plt
import matplotlib
matplotlib.use("Agg")

try:
    import wandb
    WANDB_AVAILABLE = True
except ImportError:
    WANDB_AVAILABLE = False

try:
    import yaml
    YAML_AVAILABLE = True
except ImportError:
    YAML_AVAILABLE = False

try:
    from sklearn.manifold import TSNE
    from sklearn.preprocessing import StandardScaler
    from sklearn.linear_model import LogisticRegression
    from sklearn.ensemble import RandomForestClassifier
    from sklearn.metrics import silhouette_score, davies_bouldin_score
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


def _set_global_reproducibility(seed: int) -> None:
    """Set global RNG seeds and deterministic backend flags."""
    random.seed(seed)
    np.random.seed(seed)
    torch.manual_seed(seed)
    if torch.cuda.is_available():
        torch.cuda.manual_seed(seed)
        torch.cuda.manual_seed_all(seed)

    if hasattr(torch.backends, "cudnn"):
        torch.backends.cudnn.deterministic = True
        torch.backends.cudnn.benchmark = False

    # Enforce deterministic algorithms where available.
    torch.use_deterministic_algorithms(True)


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

    def __getitem__(self, idx: int) -> Dict:
        return {
            "input_ids": self.input_ids[idx],
            "attention_mask": self.attention_mask[idx],
            "label": self.labels[idx],
        }


class SequenceAutoEncoder(nn.Module):
    """Transformer encoder with reconstruction decoder head."""

    def __init__(self, vocab_size: int, d_model: int = 128, nhead: int = 4, num_layers: int = 2, max_len: int = 1024):
        super().__init__()
        self.d_model = d_model
        self.vocab_size = vocab_size
        self.max_len = max_len

        self.embedding = nn.Embedding(vocab_size, d_model, padding_idx=0)
        self.pos_encoding = nn.Embedding(max_len, d_model)

        encoder_layer = nn.TransformerEncoderLayer(
            d_model=d_model,
            nhead=nhead,
            dim_feedforward=4 * d_model,
            batch_first=True,
            norm_first=True,
        )
        self.encoder = nn.TransformerEncoder(encoder_layer, num_layers=num_layers)
        self.decode_head = nn.Linear(d_model, vocab_size)
        self.projection_head = nn.Sequential(
            nn.Linear(d_model, d_model),
            nn.GELU(),
            nn.Linear(d_model, d_model),
        )

    def encode_sequence(self, input_ids: torch.Tensor, attention_mask: torch.Tensor) -> torch.Tensor:
        """Return token-level contextual sequence embeddings [B, L, D]."""
        embeds = self.embedding(input_ids)
        pos_ids = torch.arange(input_ids.shape[1], device=input_ids.device).unsqueeze(0)
        pos_embeds = self.pos_encoding(pos_ids)
        embeds = embeds + pos_embeds
        return self.encoder(embeds, src_key_padding_mask=~attention_mask.bool())

    def forward(self, input_ids: torch.Tensor, attention_mask: torch.Tensor) -> torch.Tensor:
        """Autoencoder forward pass (encode + decode)."""
        encoded = self.encode_sequence(input_ids, attention_mask)
        logits = self.decode_head(encoded)
        return logits

    def encode(self, input_ids: torch.Tensor, attention_mask: torch.Tensor) -> torch.Tensor:
        """Get patient-level embeddings (masked mean pooling)."""
        encoded = self.encode_sequence(input_ids, attention_mask)

        # Masked mean pooling: average only non-padded tokens
        mask_expanded = attention_mask.unsqueeze(-1).expand(encoded.size()).float()
        sum_embeds = (encoded * mask_expanded).sum(dim=1)
        sum_mask = mask_expanded.sum(dim=1)
        patient_embeds = sum_embeds / sum_mask.clamp(min=1e-9)
        return patient_embeds

    def project(self, embeddings: torch.Tensor) -> torch.Tensor:
        """Projection head used for contrastive SSL objectives."""
        return self.projection_head(embeddings)


# ============================================================================
# Q3.1: Pretraining and Linear Probes
# ============================================================================

class BaseSSLEncoder(ABC):
    """Abstract base class for self-supervised learning encoders."""

    @abstractmethod
    def pretrain(self, train_loader: DataLoader, val_loader: DataLoader, device: torch.device, epochs: int, lr: float):
        """Pretraining loop."""
        pass

    @abstractmethod
    def get_encoder(self) -> nn.Module:
        """Return the pretrained encoder."""
        pass

    @abstractmethod
    def monitoring_metrics(self) -> Dict:
        """Return monitoring metrics from pretraining."""
        pass


class TokenAugmentationMixin:
    """Token-space augmentations for denoising and contrastive SSL."""

    @staticmethod
    def _corrupt_tokens(
        input_ids: torch.Tensor,
        attention_mask: torch.Tensor,
        mask_ratio: float,
        pad_token_id: int,
        unk_token_id: int,
        special_token_ids: List[int],
    ) -> torch.Tensor:
        corrupted = input_ids.clone()
        valid_positions = attention_mask.bool().clone()

        for special_id in special_token_ids:
            valid_positions &= input_ids.ne(special_id)
        valid_positions &= input_ids.ne(pad_token_id)

        mask_probs = torch.rand_like(corrupted.float())
        selected = (mask_probs < mask_ratio) & valid_positions
        corrupted[selected] = unk_token_id
        return corrupted

    @staticmethod
    def _drop_tokens(
        input_ids: torch.Tensor,
        attention_mask: torch.Tensor,
        drop_ratio: float,
        pad_token_id: int,
        special_token_ids: List[int],
    ) -> Tuple[torch.Tensor, torch.Tensor]:
        dropped_ids = input_ids.clone()
        dropped_mask = attention_mask.clone()

        valid_positions = attention_mask.bool().clone()
        for special_id in special_token_ids:
            valid_positions &= input_ids.ne(special_id)
        valid_positions &= input_ids.ne(pad_token_id)

        drop_probs = torch.rand_like(dropped_ids.float())
        to_drop = (drop_probs < drop_ratio) & valid_positions
        dropped_ids[to_drop] = pad_token_id
        dropped_mask[to_drop] = 0
        return dropped_ids, dropped_mask

    def _build_contrastive_view(
        self,
        input_ids: torch.Tensor,
        attention_mask: torch.Tensor,
        mask_ratio: float,
        drop_ratio: float,
        pad_token_id: int,
        unk_token_id: int,
        special_token_ids: List[int],
    ) -> Tuple[torch.Tensor, torch.Tensor]:
        view_ids = self._corrupt_tokens(
            input_ids=input_ids,
            attention_mask=attention_mask,
            mask_ratio=mask_ratio,
            pad_token_id=pad_token_id,
            unk_token_id=unk_token_id,
            special_token_ids=special_token_ids,
        )
        view_ids, view_mask = self._drop_tokens(
            input_ids=view_ids,
            attention_mask=attention_mask,
            drop_ratio=drop_ratio,
            pad_token_id=pad_token_id,
            special_token_ids=special_token_ids,
        )
        return view_ids, view_mask


class RepresentationMonitoringMixin:
    """Utilities to monitor embedding quality during pretraining."""

    @staticmethod
    def _collapse_metrics(embeddings: np.ndarray, active_dim_threshold: float = 1e-3) -> Dict[str, float]:
        if embeddings is None or embeddings.size == 0:
            return {
                "feature_std_mean": float("nan"),
                "feature_std_min": float("nan"),
                "active_dims": float("nan"),
                "effective_rank": float("nan"),
                "mean_abs_cosine_offdiag": float("nan"),
            }

        emb = np.asarray(embeddings, dtype=np.float64)
        if emb.ndim != 2 or emb.shape[0] == 0 or emb.shape[1] == 0:
            return {
                "feature_std_mean": float("nan"),
                "feature_std_min": float("nan"),
                "active_dims": float("nan"),
                "effective_rank": float("nan"),
                "mean_abs_cosine_offdiag": float("nan"),
            }

        feature_std = emb.std(axis=0)
        feature_std_mean = float(np.mean(feature_std))
        feature_std_min = float(np.min(feature_std))
        active_dims = float(np.sum(feature_std > active_dim_threshold))

        centered = emb - emb.mean(axis=0, keepdims=True)
        effective_rank = float("nan")
        try:
            singular_values = np.linalg.svd(centered, compute_uv=False)
            s_sum = float(np.sum(singular_values))
            if s_sum > 0.0:
                p = singular_values / s_sum
                p = p[p > 0]
                entropy = -float(np.sum(p * np.log(p)))
                effective_rank = float(np.exp(entropy))
            else:
                effective_rank = 0.0
        except Exception:
            effective_rank = float("nan")

        mean_abs_cosine_offdiag = float("nan")
        try:
            norms = np.linalg.norm(emb, axis=1, keepdims=True)
            normed = emb / np.clip(norms, 1e-12, None)
            cosine = normed @ normed.T
            if cosine.shape[0] > 1:
                offdiag_mask = ~np.eye(cosine.shape[0], dtype=bool)
                mean_abs_cosine_offdiag = float(np.mean(np.abs(cosine[offdiag_mask])))
        except Exception:
            mean_abs_cosine_offdiag = float("nan")

        return {
            "feature_std_mean": feature_std_mean,
            "feature_std_min": feature_std_min,
            "active_dims": active_dims,
            "effective_rank": effective_rank,
            "mean_abs_cosine_offdiag": mean_abs_cosine_offdiag,
        }

    @staticmethod
    def _centroid_distance(embeddings: np.ndarray, labels: np.ndarray) -> float:
        unique = np.unique(labels)
        if len(unique) < 2:
            return float("nan")
        centroids = []
        for label in unique:
            centroids.append(embeddings[labels == label].mean(axis=0))
        if len(centroids) == 2:
            return float(np.linalg.norm(centroids[0] - centroids[1]))
        distances = []
        for i in range(len(centroids)):
            for j in range(i + 1, len(centroids)):
                distances.append(np.linalg.norm(centroids[i] - centroids[j]))
        return float(np.mean(distances)) if distances else float("nan")

    @staticmethod
    def _probe_auroc_from_embeddings(embeddings: np.ndarray, labels: np.ndarray) -> float:
        if len(np.unique(labels)) < 2 or len(labels) < 20:
            return float("nan")

        rng = np.random.default_rng(42)
        idx = np.arange(len(labels))
        rng.shuffle(idx)
        split = int(0.7 * len(idx))
        train_idx = idx[:split]
        eval_idx = idx[split:]
        if len(eval_idx) == 0:
            return float("nan")

        if SKLEARN_AVAILABLE:
            clf = LogisticRegression(max_iter=1000, random_state=42)
            clf.fit(embeddings[train_idx], labels[train_idx])
            scores = clf.predict_proba(embeddings[eval_idx])[:, 1]
            return _binary_auroc(labels[eval_idx], scores)

        weights = np.zeros(embeddings.shape[1], dtype=np.float64)
        bias = 0.0
        x_train = embeddings[train_idx]
        y_train = labels[train_idx].astype(np.float64)
        lr = 0.05
        for _ in range(80):
            logits = x_train @ weights + bias
            probs = 1.0 / (1.0 + np.exp(-logits))
            grad_w = (x_train.T @ (probs - y_train)) / len(y_train)
            grad_b = float(np.mean(probs - y_train))
            weights -= lr * grad_w
            bias -= lr * grad_b
        logits_eval = embeddings[eval_idx] @ weights + bias
        probs_eval = 1.0 / (1.0 + np.exp(-logits_eval))
        return _binary_auroc(labels[eval_idx], probs_eval)

    def _monitor_representation_quality(
        self,
        model: SequenceAutoEncoder,
        dataloader: DataLoader,
        device: torch.device,
        max_samples: int = 1200,
    ) -> Dict[str, float]:
        model.eval()
        emb_list: List[np.ndarray] = []
        lab_list: List[np.ndarray] = []
        seen = 0

        with torch.no_grad():
            for batch in dataloader:
                ids = batch["input_ids"].to(device)
                mask = batch["attention_mask"].to(device)
                labels = batch["label"].cpu().numpy()
                emb = model.encode(ids, mask).cpu().numpy()
                emb_list.append(emb)
                lab_list.append(labels)
                seen += len(labels)
                if seen >= max_samples:
                    break

        if not emb_list:
            return {
                "silhouette": float("nan"),
                "centroid_distance": float("nan"),
                "probe_auroc": float("nan"),
                "feature_std_mean": float("nan"),
                "feature_std_min": float("nan"),
                "active_dims": float("nan"),
                "effective_rank": float("nan"),
                "mean_abs_cosine_offdiag": float("nan"),
            }

        embeddings = np.concatenate(emb_list, axis=0)[:max_samples]
        labels_np = np.concatenate(lab_list, axis=0)[:max_samples]
        collapse_metrics = self._collapse_metrics(embeddings)

        silhouette_val = float("nan")
        if SKLEARN_AVAILABLE and len(np.unique(labels_np)) > 1:
            try:
                silhouette_val = float(silhouette_score(embeddings, labels_np))
            except Exception:
                silhouette_val = float("nan")

        centroid_distance = self._centroid_distance(embeddings, labels_np)
        probe_auroc = self._probe_auroc_from_embeddings(embeddings, labels_np)
        return {
            "silhouette": silhouette_val,
            "centroid_distance": centroid_distance,
            "probe_auroc": probe_auroc,
            **collapse_metrics,
        }


class InfoNCELossMixin:
    """Shared InfoNCE loss implementation used by contrastive SSL trainers."""

    @staticmethod
    def _info_nce_loss(
        z1: torch.Tensor,
        z2: torch.Tensor,
        temperature: float,
        hard_negative_k: int = 0,
    ) -> torch.Tensor:
        z1 = F.normalize(z1, dim=1)
        z2 = F.normalize(z2, dim=1)
        reps = torch.cat([z1, z2], dim=0)
        similarity = torch.matmul(reps, reps.T) / temperature

        batch_size = z1.shape[0]
        labels = torch.arange(batch_size, device=z1.device)
        labels = torch.cat([labels + batch_size, labels], dim=0)

        diag_mask = torch.eye(2 * batch_size, device=z1.device, dtype=torch.bool)
        similarity = similarity.masked_fill(diag_mask, float("-inf"))

        if hard_negative_k > 0:
            pos_idx = labels
            mine_scores = similarity.clone()
            mine_scores[torch.arange(2 * batch_size, device=z1.device), pos_idx] = float("-inf")
            k = min(hard_negative_k, (2 * batch_size) - 2)
            if k > 0:
                hard_neg_idx = torch.topk(mine_scores, k=k, dim=1).indices
                similarity.scatter_(1, hard_neg_idx, float("-inf"))

        return F.cross_entropy(similarity, labels)


class ContrastiveObjectiveMixin(TokenAugmentationMixin, InfoNCELossMixin):
    """Reusable contrastive objective utilities for TS2Vec-like multi-scale learning."""

    def _random_crop_batch(
        self,
        input_ids: torch.Tensor,
        attention_mask: torch.Tensor,
        ratio: float,
        pad_token_id: int,
    ) -> Tuple[torch.Tensor, torch.Tensor]:
        batch_size, seq_len = input_ids.shape
        out_ids = torch.full_like(input_ids, pad_token_id)
        out_mask = torch.zeros_like(attention_mask)

        valid_lengths = attention_mask.sum(dim=1).long().clamp(min=4)
        for b in range(batch_size):
            vlen = int(valid_lengths[b].item())
            crop_len = max(4, int(vlen * ratio))
            crop_len = min(crop_len, vlen)
            max_start = max(0, vlen - crop_len)
            start = int(torch.randint(low=0, high=max_start + 1, size=(1,), device=input_ids.device).item())

            segment = input_ids[b, start : start + crop_len]
            out_ids[b, :crop_len] = segment
            out_mask[b, :crop_len] = 1

        return out_ids, out_mask

    def _contrastive_branch_loss(
        self,
        input_ids: torch.Tensor,
        attention_mask: torch.Tensor,
        model: SequenceAutoEncoder,
    ) -> Tuple[torch.Tensor, torch.Tensor]:
        contrast_losses: List[torch.Tensor] = []

        for ratio in self.multiscale_ratios:
            if ratio < 0.999:
                base_ids, base_mask = self._random_crop_batch(
                    input_ids=input_ids,
                    attention_mask=attention_mask,
                    ratio=ratio,
                    pad_token_id=self.pad_token_id,
                )
            else:
                base_ids, base_mask = input_ids, attention_mask

            v1_ids, v1_mask = self._build_contrastive_view(
                base_ids,
                base_mask,
                self.mask_ratio,
                self.drop_ratio,
                self.pad_token_id,
                self.unk_token_id,
                self.special_token_ids,
            )
            v2_ids, v2_mask = self._build_contrastive_view(
                base_ids,
                base_mask,
                self.mask_ratio,
                self.drop_ratio,
                self.pad_token_id,
                self.unk_token_id,
                self.special_token_ids,
            )

            emb1 = model.encode(v1_ids, v1_mask)
            emb2 = model.encode(v2_ids, v2_mask)
            proj1 = model.project(emb1)
            proj2 = model.project(emb2)
            contrast_losses.append(
                self._info_nce_loss(
                    proj1,
                    proj2,
                    temperature=self.temperature,
                    hard_negative_k=self.hard_negative_k,
                )
            )

        contrast_loss = torch.stack(contrast_losses).mean() if contrast_losses else torch.tensor(0.0, device=input_ids.device)

        tf_loss = torch.tensor(0.0, device=input_ids.device)
        if self.time_freq_weight > 0.0:
            seq = model.encode_sequence(input_ids, attention_mask)
            mask_exp = attention_mask.unsqueeze(-1).float()
            time_repr = (seq * mask_exp).sum(dim=1) / mask_exp.sum(dim=1).clamp(min=1e-9)

            freq_repr = torch.fft.rfft(seq, dim=1).abs().mean(dim=1)
            time_repr = F.normalize(time_repr, dim=1)
            freq_repr = F.normalize(freq_repr, dim=1)
            tf_loss = F.mse_loss(time_repr, freq_repr)

        return contrast_loss, tf_loss


class AutoencoderPretrainer(BaseSSLEncoder, TokenAugmentationMixin, RepresentationMonitoringMixin):
    """Denoising autoencoder SSL using masked token reconstruction."""

    def __init__(
        self,
        model: SequenceAutoEncoder,
        pad_token_id: int,
        unk_token_id: int,
        bos_token_id: int,
        eos_token_id: int,
        mask_ratio: float = 0.2,
        drop_ratio: float = 0.1,
        monitor_every: int = 1,
        early_stop_patience: int = 0,
        early_stop_min_delta: float = 0.0,
        restore_best_weights: bool = True,
    ):
        self.model = model
        self.pad_token_id = pad_token_id
        self.unk_token_id = unk_token_id
        self.special_token_ids = [bos_token_id, eos_token_id]
        self.mask_ratio = mask_ratio
        self.drop_ratio = drop_ratio
        self.monitor_every = max(1, monitor_every)
        self.early_stop_patience = max(0, int(early_stop_patience))
        self.early_stop_min_delta = max(0.0, float(early_stop_min_delta))
        self.restore_best_weights = bool(restore_best_weights)
        self.train_losses: List[float] = []
        self.val_losses: List[float] = []
        self.rep_monitor: List[Dict[str, float]] = []
        self.best_val_loss: float = float("inf")
        self.best_epoch: int = 0
        self.early_stopped: bool = False

    def pretrain(self, train_loader: DataLoader, val_loader: DataLoader, device: torch.device, epochs: int, lr: float):
        optimizer = torch.optim.Adam(self.model.parameters(), lr=lr)
        criterion = nn.CrossEntropyLoss(ignore_index=self.pad_token_id)
        epochs_without_improvement = 0
        best_state = None

        for epoch in range(epochs):
            self.model.train()
            train_loss = 0.0
            for batch in train_loader:
                input_ids = batch["input_ids"].to(device)
                attention_mask = batch["attention_mask"].to(device)

                corrupted = self._corrupt_tokens(
                    input_ids=input_ids,
                    attention_mask=attention_mask,
                    mask_ratio=self.mask_ratio,
                    pad_token_id=self.pad_token_id,
                    unk_token_id=self.unk_token_id,
                    special_token_ids=self.special_token_ids,
                )
                corrupted, corrupted_mask = self._drop_tokens(
                    input_ids=corrupted,
                    attention_mask=attention_mask,
                    drop_ratio=self.drop_ratio,
                    pad_token_id=self.pad_token_id,
                    special_token_ids=self.special_token_ids,
                )

                logits = self.model(corrupted, corrupted_mask)
                loss = criterion(logits.view(-1, self.model.vocab_size), input_ids.view(-1))

                optimizer.zero_grad()
                loss.backward()
                optimizer.step()
                train_loss += float(loss.item())

            train_loss /= max(1, len(train_loader))
            self.train_losses.append(train_loss)

            self.model.eval()
            val_loss = 0.0
            with torch.no_grad():
                for batch in val_loader:
                    input_ids = batch["input_ids"].to(device)
                    attention_mask = batch["attention_mask"].to(device)
                    corrupted = self._corrupt_tokens(
                        input_ids=input_ids,
                        attention_mask=attention_mask,
                        mask_ratio=self.mask_ratio,
                        pad_token_id=self.pad_token_id,
                        unk_token_id=self.unk_token_id,
                        special_token_ids=self.special_token_ids,
                    )
                    corrupted, corrupted_mask = self._drop_tokens(
                        input_ids=corrupted,
                        attention_mask=attention_mask,
                        drop_ratio=self.drop_ratio,
                        pad_token_id=self.pad_token_id,
                        special_token_ids=self.special_token_ids,
                    )
                    logits = self.model(corrupted, corrupted_mask)
                    loss = criterion(logits.view(-1, self.model.vocab_size), input_ids.view(-1))
                    val_loss += float(loss.item())

            val_loss /= max(1, len(val_loader))
            self.val_losses.append(val_loss)

            improved = val_loss < (self.best_val_loss - self.early_stop_min_delta)
            if improved:
                self.best_val_loss = val_loss
                self.best_epoch = epoch + 1
                epochs_without_improvement = 0
                if self.restore_best_weights:
                    best_state = {k: v.detach().cpu().clone() for k, v in self.model.state_dict().items()}
            else:
                epochs_without_improvement += 1

            epoch_msg = f"Epoch {epoch + 1}/{epochs} | Denoise Train Loss: {train_loss:.4f} | Denoise Val Loss: {val_loss:.4f}"

            if (epoch + 1) % self.monitor_every == 0:
                rep = self._monitor_representation_quality(self.model, val_loader, device)
                rep["epoch"] = epoch + 1
                self.rep_monitor.append(rep)
                epoch_msg += (
                    f" | Probe AUROC={rep['probe_auroc']:.4f}"
                    f" | Silhouette={rep['silhouette']:.4f}"
                    f" | CentroidDist={rep['centroid_distance']:.4f}"
                )

            print(epoch_msg)

            if self.early_stop_patience > 0 and epochs_without_improvement >= self.early_stop_patience:
                self.early_stopped = True
                print(
                    f"Early stopping triggered at epoch {epoch + 1}; "
                    f"best val loss {self.best_val_loss:.4f} at epoch {self.best_epoch}."
                )
                break

        if self.restore_best_weights and best_state is not None:
            self.model.load_state_dict(best_state)

    def get_encoder(self) -> nn.Module:
        return self.model

    def monitoring_metrics(self) -> Dict:
        return {
            "train_losses": self.train_losses,
            "val_losses": self.val_losses,
            "representation_monitor": self.rep_monitor,
            "best_val_loss": self.best_val_loss,
            "best_epoch": self.best_epoch,
            "early_stopped": self.early_stopped,
        }


class ContrastivePretrainer(BaseSSLEncoder, RepresentationMonitoringMixin, ContrastiveObjectiveMixin):
    """InfoNCE-based contrastive SSL pretrainer for time-series token sequences."""

    def __init__(
        self,
        model: SequenceAutoEncoder,
        pad_token_id: int,
        unk_token_id: int,
        bos_token_id: int,
        eos_token_id: int,
        mask_ratio: float = 0.15,
        drop_ratio: float = 0.1,
        temperature: float = 0.1,
        multiscale_ratios: Optional[List[float]] = None,
        hard_negative_k: int = 0,
        time_freq_weight: float = 0.0,
        monitor_every: int = 1,
        early_stop_patience: int = 0,
        early_stop_min_delta: float = 0.0,
        restore_best_weights: bool = True,
    ):
        self.model = model
        self.pad_token_id = pad_token_id
        self.unk_token_id = unk_token_id
        self.special_token_ids = [bos_token_id, eos_token_id]
        self.mask_ratio = mask_ratio
        self.drop_ratio = drop_ratio
        self.temperature = temperature
        self.multiscale_ratios = multiscale_ratios or [1.0, 0.75, 0.5]
        self.hard_negative_k = max(0, hard_negative_k)
        self.time_freq_weight = max(0.0, time_freq_weight)
        self.monitor_every = max(1, monitor_every)
        self.early_stop_patience = max(0, int(early_stop_patience))
        self.early_stop_min_delta = max(0.0, float(early_stop_min_delta))
        self.restore_best_weights = bool(restore_best_weights)
        self.train_losses: List[float] = []
        self.val_losses: List[float] = []
        self.time_freq_train_losses: List[float] = []
        self.time_freq_val_losses: List[float] = []
        self.rep_monitor: List[Dict[str, float]] = []
        self.best_val_loss: float = float("inf")
        self.best_epoch: int = 0
        self.early_stopped: bool = False

    def pretrain(self, train_loader: DataLoader, val_loader: DataLoader, device: torch.device, epochs: int, lr: float):
        optimizer = torch.optim.Adam(self.model.parameters(), lr=lr)
        epochs_without_improvement = 0
        best_state = None

        for epoch in range(epochs):
            self.model.train()
            train_loss = 0.0
            train_tf = 0.0
            for batch in train_loader:
                input_ids = batch["input_ids"].to(device)
                attention_mask = batch["attention_mask"].to(device)

                contrast_loss, tf_loss = self._contrastive_branch_loss(input_ids, attention_mask, self.model)
                loss = contrast_loss + self.time_freq_weight * tf_loss
                optimizer.zero_grad()
                loss.backward()
                optimizer.step()
                train_loss += float(loss.item())
                train_tf += float(tf_loss.item())

            train_loss /= max(1, len(train_loader))
            train_tf /= max(1, len(train_loader))
            self.train_losses.append(train_loss)
            self.time_freq_train_losses.append(train_tf)

            self.model.eval()
            val_loss = 0.0
            val_tf = 0.0
            with torch.no_grad():
                for batch in val_loader:
                    input_ids = batch["input_ids"].to(device)
                    attention_mask = batch["attention_mask"].to(device)
                    contrast_loss, tf_loss = self._contrastive_branch_loss(input_ids, attention_mask, self.model)
                    val_loss += float((contrast_loss + self.time_freq_weight * tf_loss).item())
                    val_tf += float(tf_loss.item())

            val_loss /= max(1, len(val_loader))
            val_tf /= max(1, len(val_loader))
            self.val_losses.append(val_loss)
            self.time_freq_val_losses.append(val_tf)

            improved = val_loss < (self.best_val_loss - self.early_stop_min_delta)
            if improved:
                self.best_val_loss = val_loss
                self.best_epoch = epoch + 1
                epochs_without_improvement = 0
                if self.restore_best_weights:
                    best_state = {k: v.detach().cpu().clone() for k, v in self.model.state_dict().items()}
            else:
                epochs_without_improvement += 1

            epoch_msg = (
                f"Epoch {epoch + 1}/{epochs} | Contrastive Train Loss: {train_loss:.4f} | Contrastive Val Loss: {val_loss:.4f}"
                f" | TF Train={train_tf:.4f} | TF Val={val_tf:.4f}"
            )
            if (epoch + 1) % self.monitor_every == 0:
                rep = self._monitor_representation_quality(self.model, val_loader, device)
                rep["epoch"] = epoch + 1
                self.rep_monitor.append(rep)
                epoch_msg += (
                    f" | Probe AUROC={rep['probe_auroc']:.4f}"
                    f" | Silhouette={rep['silhouette']:.4f}"
                    f" | CentroidDist={rep['centroid_distance']:.4f}"
                )
            print(epoch_msg)

            if self.early_stop_patience > 0 and epochs_without_improvement >= self.early_stop_patience:
                self.early_stopped = True
                print(
                    f"Early stopping triggered at epoch {epoch + 1}; "
                    f"best val loss {self.best_val_loss:.4f} at epoch {self.best_epoch}."
                )
                break

        if self.restore_best_weights and best_state is not None:
            self.model.load_state_dict(best_state)

    def get_encoder(self) -> nn.Module:
        return self.model

    def monitoring_metrics(self) -> Dict:
        return {
            "train_losses": self.train_losses,
            "val_losses": self.val_losses,
            "time_freq_train_losses": self.time_freq_train_losses,
            "time_freq_val_losses": self.time_freq_val_losses,
            "representation_monitor": self.rep_monitor,
            "best_val_loss": self.best_val_loss,
            "best_epoch": self.best_epoch,
            "early_stopped": self.early_stopped,
        }


class HybridPretrainer(BaseSSLEncoder, RepresentationMonitoringMixin, ContrastiveObjectiveMixin):
    """Hybrid SSL: denoising reconstruction + InfoNCE contrastive objective."""

    def __init__(
        self,
        model: SequenceAutoEncoder,
        pad_token_id: int,
        unk_token_id: int,
        bos_token_id: int,
        eos_token_id: int,
        mask_ratio: float = 0.2,
        drop_ratio: float = 0.1,
        temperature: float = 0.1,
        reconstruction_weight: float = 1.0,
        contrastive_weight: float = 1.0,
        multiscale_ratios: Optional[List[float]] = None,
        hard_negative_k: int = 0,
        time_freq_weight: float = 0.0,
        monitor_every: int = 1,
        early_stop_patience: int = 0,
        early_stop_min_delta: float = 0.0,
        restore_best_weights: bool = True,
    ):
        self.model = model
        self.pad_token_id = pad_token_id
        self.unk_token_id = unk_token_id
        self.special_token_ids = [bos_token_id, eos_token_id]
        self.mask_ratio = mask_ratio
        self.drop_ratio = drop_ratio
        self.temperature = temperature
        self.reconstruction_weight = reconstruction_weight
        self.contrastive_weight = contrastive_weight
        self.multiscale_ratios = multiscale_ratios or [1.0, 0.75, 0.5]
        self.hard_negative_k = max(0, hard_negative_k)
        self.time_freq_weight = max(0.0, time_freq_weight)
        self.monitor_every = max(1, monitor_every)
        self.early_stop_patience = max(0, int(early_stop_patience))
        self.early_stop_min_delta = max(0.0, float(early_stop_min_delta))
        self.restore_best_weights = bool(restore_best_weights)

        self.train_losses: List[float] = []
        self.val_losses: List[float] = []
        self.recon_train_losses: List[float] = []
        self.recon_val_losses: List[float] = []
        self.contrast_train_losses: List[float] = []
        self.contrast_val_losses: List[float] = []
        self.time_freq_train_losses: List[float] = []
        self.time_freq_val_losses: List[float] = []
        self.rep_monitor: List[Dict[str, float]] = []
        self.best_val_loss: float = float("inf")
        self.best_epoch: int = 0
        self.early_stopped: bool = False

    def pretrain(self, train_loader: DataLoader, val_loader: DataLoader, device: torch.device, epochs: int, lr: float):
        optimizer = torch.optim.Adam(self.model.parameters(), lr=lr)
        recon_criterion = nn.CrossEntropyLoss(ignore_index=self.pad_token_id)
        epochs_without_improvement = 0
        best_state = None

        for epoch in range(epochs):
            self.model.train()
            total_train = 0.0
            recon_train = 0.0
            contrast_train = 0.0
            tf_train = 0.0
            for batch in train_loader:
                input_ids = batch["input_ids"].to(device)
                attention_mask = batch["attention_mask"].to(device)

                # Denoising reconstruction branch
                corrupted = self._corrupt_tokens(
                    input_ids=input_ids,
                    attention_mask=attention_mask,
                    mask_ratio=self.mask_ratio,
                    pad_token_id=self.pad_token_id,
                    unk_token_id=self.unk_token_id,
                    special_token_ids=self.special_token_ids,
                )
                corrupted, corrupted_mask = self._drop_tokens(
                    input_ids=corrupted,
                    attention_mask=attention_mask,
                    drop_ratio=self.drop_ratio,
                    pad_token_id=self.pad_token_id,
                    special_token_ids=self.special_token_ids,
                )
                recon_logits = self.model(corrupted, corrupted_mask)
                recon_loss = recon_criterion(recon_logits.view(-1, self.model.vocab_size), input_ids.view(-1))

                # Contrastive branch
                contrast_loss, tf_loss = self._contrastive_branch_loss(input_ids, attention_mask, self.model)

                loss = (
                    self.reconstruction_weight * recon_loss
                    + self.contrastive_weight * contrast_loss
                    + self.time_freq_weight * tf_loss
                )

                optimizer.zero_grad()
                loss.backward()
                optimizer.step()

                total_train += float(loss.item())
                recon_train += float(recon_loss.item())
                contrast_train += float(contrast_loss.item())
                tf_train += float(tf_loss.item())

            train_den = max(1, len(train_loader))
            total_train /= train_den
            recon_train /= train_den
            contrast_train /= train_den
            tf_train /= train_den
            self.train_losses.append(total_train)
            self.recon_train_losses.append(recon_train)
            self.contrast_train_losses.append(contrast_train)
            self.time_freq_train_losses.append(tf_train)

            self.model.eval()
            total_val = 0.0
            recon_val = 0.0
            contrast_val = 0.0
            tf_val = 0.0
            with torch.no_grad():
                for batch in val_loader:
                    input_ids = batch["input_ids"].to(device)
                    attention_mask = batch["attention_mask"].to(device)

                    corrupted = self._corrupt_tokens(
                        input_ids=input_ids,
                        attention_mask=attention_mask,
                        mask_ratio=self.mask_ratio,
                        pad_token_id=self.pad_token_id,
                        unk_token_id=self.unk_token_id,
                        special_token_ids=self.special_token_ids,
                    )
                    corrupted, corrupted_mask = self._drop_tokens(
                        input_ids=corrupted,
                        attention_mask=attention_mask,
                        drop_ratio=self.drop_ratio,
                        pad_token_id=self.pad_token_id,
                        special_token_ids=self.special_token_ids,
                    )
                    recon_logits = self.model(corrupted, corrupted_mask)
                    recon_loss = recon_criterion(recon_logits.view(-1, self.model.vocab_size), input_ids.view(-1))

                    contrast_loss, tf_loss = self._contrastive_branch_loss(input_ids, attention_mask, self.model)

                    total_loss = (
                        self.reconstruction_weight * recon_loss
                        + self.contrastive_weight * contrast_loss
                        + self.time_freq_weight * tf_loss
                    )
                    total_val += float(total_loss.item())
                    recon_val += float(recon_loss.item())
                    contrast_val += float(contrast_loss.item())
                    tf_val += float(tf_loss.item())

            val_den = max(1, len(val_loader))
            total_val /= val_den
            recon_val /= val_den
            contrast_val /= val_den
            tf_val /= val_den
            self.val_losses.append(total_val)
            self.recon_val_losses.append(recon_val)
            self.contrast_val_losses.append(contrast_val)
            self.time_freq_val_losses.append(tf_val)

            improved = total_val < (self.best_val_loss - self.early_stop_min_delta)
            if improved:
                self.best_val_loss = total_val
                self.best_epoch = epoch + 1
                epochs_without_improvement = 0
                if self.restore_best_weights:
                    best_state = {k: v.detach().cpu().clone() for k, v in self.model.state_dict().items()}
            else:
                epochs_without_improvement += 1

            epoch_msg = (
                f"Epoch {epoch + 1}/{epochs} | Hybrid Train Total: {total_train:.4f} (Recon={recon_train:.4f}, Contrast={contrast_train:.4f})"
                f" | Hybrid Val Total: {total_val:.4f} (Recon={recon_val:.4f}, Contrast={contrast_val:.4f})"
                f" | TF Train={tf_train:.4f} | TF Val={tf_val:.4f}"
            )

            if (epoch + 1) % self.monitor_every == 0:
                rep = self._monitor_representation_quality(self.model, val_loader, device)
                rep["epoch"] = epoch + 1
                self.rep_monitor.append(rep)
                epoch_msg += (
                    f" | Probe AUROC={rep['probe_auroc']:.4f}"
                    f" | Silhouette={rep['silhouette']:.4f}"
                    f" | CentroidDist={rep['centroid_distance']:.4f}"
                )

            print(epoch_msg)

            if self.early_stop_patience > 0 and epochs_without_improvement >= self.early_stop_patience:
                self.early_stopped = True
                print(
                    f"Early stopping triggered at epoch {epoch + 1}; "
                    f"best val loss {self.best_val_loss:.4f} at epoch {self.best_epoch}."
                )
                break

        if self.restore_best_weights and best_state is not None:
            self.model.load_state_dict(best_state)

    def get_encoder(self) -> nn.Module:
        return self.model

    def monitoring_metrics(self) -> Dict:
        return {
            "train_losses": self.train_losses,
            "val_losses": self.val_losses,
            "recon_train_losses": self.recon_train_losses,
            "recon_val_losses": self.recon_val_losses,
            "contrast_train_losses": self.contrast_train_losses,
            "contrast_val_losses": self.contrast_val_losses,
            "time_freq_train_losses": self.time_freq_train_losses,
            "time_freq_val_losses": self.time_freq_val_losses,
            "representation_monitor": self.rep_monitor,
            "best_val_loss": self.best_val_loss,
            "best_epoch": self.best_epoch,
            "early_stopped": self.early_stopped,
        }


# ============================================================================
# Q3.2: Label Scarcity & Supervised Models
# ============================================================================

class LinearProbe:
    """Q3.1 Step 2 & Q3.2: Logistic regression on frozen embeddings."""

    def __init__(self):
        self.model = None
        self.scaler = None

    def train(self, embeddings: np.ndarray, labels: np.ndarray):
        """Train logistic regression on embeddings."""
        if not SKLEARN_AVAILABLE:
            raise ImportError("scikit-learn required for LogisticRegression")
        self.scaler = StandardScaler()
        train_emb = self.scaler.fit_transform(embeddings)
        self.model = LogisticRegression(max_iter=5000, random_state=42, solver="lbfgs")
        self.model.fit(train_emb, labels)

    def predict_proba(self, embeddings: np.ndarray) -> np.ndarray:
        """Get probability predictions."""
        if self.scaler is None:
            raise RuntimeError("LinearProbe must be trained before prediction.")
        emb = self.scaler.transform(embeddings)
        return self.model.predict_proba(emb)[:, 1]

    def predict(self, embeddings: np.ndarray) -> np.ndarray:
        """Get class predictions."""
        if self.scaler is None:
            raise RuntimeError("LinearProbe must be trained before prediction.")
        emb = self.scaler.transform(embeddings)
        return self.model.predict(emb)


class RandomForestModel:
    """Supervised RF model for Q2-equivalent comparison."""

    def __init__(self, n_estimators: int = 100):
        if not SKLEARN_AVAILABLE:
            raise ImportError("scikit-learn required for RandomForest")
        self.model = RandomForestClassifier(n_estimators=n_estimators, random_state=42, n_jobs=-1)

    def train(self, embeddings: np.ndarray, labels: np.ndarray):
        """Train on embeddings."""
        self.model.fit(embeddings, labels)

    def predict_proba(self, embeddings: np.ndarray) -> np.ndarray:
        """Get probability predictions."""
        return self.model.predict_proba(embeddings)[:, 1]

    def predict(self, embeddings: np.ndarray) -> np.ndarray:
        """Get class predictions."""
        return self.model.predict(embeddings)


# ============================================================================
# Metrics & Utilities
# ============================================================================

def _labels_to_numpy(labels: torch.Tensor | np.ndarray) -> np.ndarray:
    """Convert labels to a NumPy array from torch or NumPy input."""
    if isinstance(labels, torch.Tensor):
        return labels.detach().cpu().numpy()
    return np.asarray(labels)


def _stratified_split_indices(labels: torch.Tensor | np.ndarray, train_frac: float = 0.7, val_frac: float = 0.15, seed: int = 42):
    """Stratified train/val/test split by label."""
    np.random.seed(seed)
    labels_np = _labels_to_numpy(labels)
    indices = np.arange(len(labels_np))

    # Stratified split
    unique_labels = np.unique(labels_np)
    train_idx, val_idx, test_idx = [], [], []

    for label in unique_labels:
        label_indices = indices[labels_np == label]
        np.random.shuffle(label_indices)

        n = len(label_indices)
        train_n = int(n * train_frac)
        val_n = int(n * val_frac)

        train_idx.extend(label_indices[:train_n])
        val_idx.extend(label_indices[train_n : train_n + val_n])
        test_idx.extend(label_indices[train_n + val_n :])

    return np.array(train_idx), np.array(val_idx), np.array(test_idx)


def _assert_disjoint_splits(train_idx: np.ndarray, val_idx: np.ndarray, test_idx: np.ndarray) -> None:
    """Fail fast if any sample index leaks across train/val/test splits."""
    train_set = set(np.asarray(train_idx).tolist())
    val_set = set(np.asarray(val_idx).tolist())
    test_set = set(np.asarray(test_idx).tolist())

    train_val_overlap = train_set.intersection(val_set)
    train_test_overlap = train_set.intersection(test_set)
    val_test_overlap = val_set.intersection(test_set)

    if train_val_overlap or train_test_overlap or val_test_overlap:
        raise ValueError(
            "Data leakage detected: train/val/test splits overlap. "
            f"overlaps(train,val)={len(train_val_overlap)}, "
            f"(train,test)={len(train_test_overlap)}, "
            f"(val,test)={len(val_test_overlap)}"
        )


def _stratified_subset_by_count(labels: torch.Tensor | np.ndarray, indices: np.ndarray, n_samples: int, seed: int = 42):
    """Subsample from indices preserving label distribution."""
    np.random.seed(seed)
    labels_np = _labels_to_numpy(labels)
    labels_subset = labels_np[indices]
    unique_labels = np.unique(labels_subset)

    subset_indices = []
    for label in unique_labels:
        label_mask = labels_subset == label
        label_indices = indices[label_mask]
        n_label = max(1, int(n_samples * (label_mask.sum() / len(labels_subset))))
        sampled = np.random.choice(label_indices, size=min(n_label, len(label_indices)), replace=False)
        subset_indices.extend(sampled)

    return np.array(subset_indices)


def _binary_auroc(y_true: np.ndarray, y_score: np.ndarray) -> float:
    """Compute AUROC using Mann-Whitney U statistic (no sklearn dependency)."""
    n_pos = (y_true == 1).sum()
    n_neg = (y_true == 0).sum()

    if n_pos == 0 or n_neg == 0:
        return float("nan")

    pos_scores = y_score[y_true == 1]
    neg_scores = y_score[y_true == 0]

    # Mann-Whitney U: count pairs where pos > neg
    comparisons = (pos_scores.reshape(-1, 1) > neg_scores.reshape(1, -1)).sum()
    ties = (pos_scores.reshape(-1, 1) == neg_scores.reshape(1, -1)).sum()
    auroc = (comparisons + 0.5 * ties) / (n_pos * n_neg)
    return float(auroc)


def _binary_auprc(y_true: np.ndarray, y_score: np.ndarray) -> float:
    """Compute AUPRC via manual trapezoidal integration."""
    n_pos = (y_true == 1).sum()
    n_total = len(y_true)

    if n_pos == 0:
        return float("nan")

    sorted_idx = np.argsort(-y_score)
    sorted_labels = y_true[sorted_idx]

    precisions = []
    recalls = []
    tp = 0
    for i in range(len(sorted_labels)):
        if sorted_labels[i] == 1:
            tp += 1
        fp = i + 1 - tp
        precision = tp / (tp + fp + 1e-9)
        recall = tp / n_pos
        precisions.append(precision)
        recalls.append(recall)

    precisions = np.array(precisions)
    recalls = np.array(recalls)

    # Trapezoidal rule
    auprc = 0.0
    for i in range(1, len(recalls)):
        auprc += (recalls[i] - recalls[i - 1]) * (precisions[i] + precisions[i - 1]) / 2
    return float(auprc)


def evaluate_metrics(y_true: np.ndarray, y_score: np.ndarray) -> Tuple[float, float]:
    """Compute AUROC and AUPRC."""
    auroc = _binary_auroc(y_true, y_score)
    auprc = _binary_auprc(y_true, y_score)
    return auroc, auprc


def get_embeddings(model: SequenceAutoEncoder, dataloader: DataLoader, device: torch.device) -> Tuple[np.ndarray, np.ndarray]:
    """Extract embeddings and labels from a dataloader."""
    model.eval()
    embeddings_list = []
    labels_list = []

    with torch.no_grad():
        for batch in dataloader:
            token_ids = batch["input_ids"].to(device)
            attention_mask = batch["attention_mask"].to(device)
            labels = batch["label"].to(device)

            embeds = model.encode(token_ids, attention_mask)
            embeddings_list.append(embeds.cpu().numpy())
            labels_list.append(labels.cpu().numpy())

    embeddings = np.concatenate(embeddings_list, axis=0)
    labels = np.concatenate(labels_list, axis=0)
    return embeddings, labels


# ============================================================================
# Q3.3: Visualization & Clustering Metrics
# ============================================================================

class RepresentationVisualizer:
    """Q3.3: Visualize learned embeddings with t-SNE and UMAP."""

    def __init__(
        self,
        output_dir: str = "./embeddings",
        tsne_perplexities: Optional[List[int]] = None,
        umap_neighbors: Optional[List[int]] = None,
        umap_min_dist: Optional[List[float]] = None,
    ):
        self.output_dir = output_dir
        self.tsne_perplexities = tsne_perplexities or [20, 30, 40, 50]
        self.umap_neighbors = umap_neighbors or [10, 15, 30, 50]
        self.umap_min_dist = umap_min_dist or [0.05, 0.1, 0.25]
        os.makedirs(output_dir, exist_ok=True)

    def visualize(self, embeddings: np.ndarray, labels: np.ndarray):
        """Create tuned t-SNE and UMAP visualizations and save best settings."""
        # Standardize
        if SKLEARN_AVAILABLE:
            scaler = StandardScaler()
            embeddings = scaler.fit_transform(embeddings)
        else:
            embeddings = (embeddings - embeddings.mean(axis=0)) / (embeddings.std(axis=0) + 1e-8)

        colors = ['blue' if label == 0 else 'orange' for label in labels]
        tuning_results: Dict[str, Dict] = {}

        # t-SNE
        if SKLEARN_AVAILABLE:
            best_tsne_score = float("-inf")
            best_tsne_perplexity = None
            best_tsne_2d = None
            for perplexity in self.tsne_perplexities:
                if perplexity >= max(5, len(embeddings) - 1):
                    continue
                try:
                    print(f"Computing t-SNE (perplexity={perplexity})...")
                    tsne = TSNE(n_components=2, random_state=42, max_iter=1000, perplexity=perplexity)
                    emb_2d = tsne.fit_transform(embeddings)
                    score = float("nan")
                    if len(np.unique(labels)) > 1:
                        score = float(silhouette_score(emb_2d, labels))
                    if np.isnan(score):
                        score = -1e9
                    if score > best_tsne_score:
                        best_tsne_score = score
                        best_tsne_perplexity = perplexity
                        best_tsne_2d = emb_2d
                except Exception as e:
                    print(f"t-SNE run failed for perplexity={perplexity}: {e}")

            if best_tsne_2d is not None:
                plt.figure(figsize=(10, 8))
                plt.scatter(best_tsne_2d[:, 0], best_tsne_2d[:, 1], c=colors, alpha=0.6, s=50)
                plt.xlabel("t-SNE 1")
                plt.ylabel("t-SNE 2")
                plt.title(
                    "Learned Embeddings - t-SNE "
                    f"(best perplexity={best_tsne_perplexity}, silhouette={best_tsne_score:.4f})"
                )
                plt.tight_layout()
                tsne_path = os.path.join(self.output_dir, "tsne_embeddings.png")
                plt.savefig(tsne_path, dpi=150, bbox_inches='tight')
                plt.close()
                print(f"Saved t-SNE plot to {tsne_path}")
                tuning_results["tsne"] = {
                    "best_perplexity": best_tsne_perplexity,
                    "best_silhouette": best_tsne_score,
                }

        # UMAP
        if UMAP_AVAILABLE:
            best_umap_score = float("-inf")
            best_umap_params = None
            best_umap_2d = None
            for neighbors in self.umap_neighbors:
                for min_dist in self.umap_min_dist:
                    try:
                        print(f"Computing UMAP (n_neighbors={neighbors}, min_dist={min_dist})...")
                        reducer = umap.UMAP(
                            n_components=2,
                            random_state=42,
                            n_neighbors=neighbors,
                            min_dist=min_dist,
                        )
                        emb_2d = reducer.fit_transform(embeddings)
                        score = float("nan")
                        if SKLEARN_AVAILABLE and len(np.unique(labels)) > 1:
                            score = float(silhouette_score(emb_2d, labels))
                        if np.isnan(score):
                            score = -1e9
                        if score > best_umap_score:
                            best_umap_score = score
                            best_umap_params = {"n_neighbors": neighbors, "min_dist": min_dist}
                            best_umap_2d = emb_2d
                    except Exception as e:
                        print(f"UMAP run failed for n_neighbors={neighbors}, min_dist={min_dist}: {e}")

            if best_umap_2d is not None:
                plt.figure(figsize=(10, 8))
                plt.scatter(best_umap_2d[:, 0], best_umap_2d[:, 1], c=colors, alpha=0.6, s=50)
                plt.xlabel("UMAP 1")
                plt.ylabel("UMAP 2")
                plt.title(
                    "Learned Embeddings - UMAP "
                    f"(best n_neighbors={best_umap_params['n_neighbors']}, min_dist={best_umap_params['min_dist']}, "
                    f"silhouette={best_umap_score:.4f})"
                )
                plt.tight_layout()
                umap_path = os.path.join(self.output_dir, "umap_embeddings.png")
                plt.savefig(umap_path, dpi=150, bbox_inches='tight')
                plt.close()
                print(f"Saved UMAP plot to {umap_path}")
                tuning_results["umap"] = {
                    "best_n_neighbors": best_umap_params["n_neighbors"],
                    "best_min_dist": best_umap_params["min_dist"],
                    "best_silhouette": best_umap_score,
                }

        if tuning_results:
            tuning_path = os.path.join(self.output_dir, "projection_tuning.json")
            with open(tuning_path, "w", encoding="utf-8") as f:
                json.dump(tuning_results, f, indent=2)
            print(f"Saved projection tuning summary to {tuning_path}")


class ClusteringMetrics:
    """Q3.3: Quantitative assessment of representation quality."""

    @staticmethod
    def silhouette_score(embeddings: np.ndarray, labels: np.ndarray) -> float:
        """Silhouette coefficient: measures cluster cohesion and separation."""
        if not SKLEARN_AVAILABLE or len(np.unique(labels)) < 2:
            return float("nan")
        try:
            return silhouette_score(embeddings, labels)
        except Exception as e:
            print(f"Silhouette score computation failed: {e}")
            return float("nan")

    @staticmethod
    def davies_bouldin_score(embeddings: np.ndarray, labels: np.ndarray) -> float:
        """Davies-Bouldin index: lower is better (0 = perfect separation)."""
        if not SKLEARN_AVAILABLE or len(np.unique(labels)) < 2:
            return float("nan")
        try:
            return davies_bouldin_score(embeddings, labels)
        except Exception as e:
            print(f"Davies-Bouldin score computation failed: {e}")
            return float("nan")

    @staticmethod
    def compute_class_separation(embeddings: np.ndarray, labels: np.ndarray) -> Dict[str, float]:
        """Assess separation between classes: intra-class variance vs inter-class distance."""
        unique_labels = np.unique(labels)
        if len(unique_labels) < 2:
            return {}

        metrics = {}
        class_centroids = {}
        for label in unique_labels:
            class_embeds = embeddings[labels == label]
            centroid = class_embeds.mean(axis=0)
            class_centroids[label] = centroid
            intra_dist = np.mean([np.linalg.norm(e - centroid) for e in class_embeds])
            metrics[f"intra_class_dist_label_{label}"] = intra_dist

        # Inter-class distance
        if len(unique_labels) == 2:
            inter_dist = np.linalg.norm(class_centroids[0] - class_centroids[1])
            metrics["inter_class_distance"] = inter_dist

        return metrics


# ============================================================================
# Data Loading and Preprocessing
# ============================================================================

def _impute_and_scale_data(
    full_df: pd.DataFrame,
    train_idx: np.ndarray,
    val_idx: np.ndarray,
    test_idx: np.ndarray,
) -> Tuple[pd.DataFrame, Dict[str, StandardScaler]]:
    """
    Apply forward-fill imputation and scaling to the data.
    
    - Forward fill imputation: Fill missing values by carrying forward the last known value per patient
    - Scaling: Fit StandardScaler only on training set (by patient), then apply to all
    
    Args:
        full_df: Wide-format DataFrame with measurements (one row per patient per time step)
        train_idx: Patient-level indices for training set
        val_idx: Patient-level indices for validation set
        test_idx: Patient-level indices for test set
    
    Returns:
        - Imputed and scaled DataFrame
        - Dictionary of fitted scalers per parameter
    """
    id_cols = ["PatientID", "In-hospital_death"]
    time_col = "Time"
    exclude_cols = id_cols + [time_col, "RecordID_x", "RecordID_y"]
    value_cols = [c for c in full_df.columns if c not in exclude_cols]
    
    # Get patient ID list (in order) from the DataFrame by taking one row per patient
    patient_list = full_df.drop_duplicates(subset="PatientID").sort_values("PatientID")["PatientID"].values
    
    # Map patient indices to actual patient IDs
    train_patient_ids = set(patient_list[train_idx])
    
    # Step 1: Forward fill imputation per patient (pandas>=2 compatible)
    df_filled = full_df.copy()
    df_filled[value_cols] = (
        df_filled.groupby("PatientID", sort=False)[value_cols]
        .ffill()
    )
    
    # Step 2: Fit scalers on training set patients only, then apply to all
    scalers: Dict[str, StandardScaler] = {}
    
    # Extract training data (all rows for training patients)
    train_data = df_filled[df_filled["PatientID"].isin(train_patient_ids)][value_cols]
    
    # Fit scalers per parameter on training data only
    for col in value_cols:
        scaler = StandardScaler()
        col_data = train_data[[col]].dropna().values
        if len(col_data) > 0:
            scaler.fit(col_data)
            scalers[col] = scaler
    
    # Apply scalers to all data
    df_scaled = df_filled.copy()
    for col in value_cols:
        if col in scalers:
            df_scaled[col] = scalers[col].transform(df_filled[[col]].values)
    
    return df_scaled, scalers


def build_tuple_token_dataset(
    parquet_path: str,
    n_value_bins: int = 10,
    max_seq_len: int = 1024,
    train_idx: Optional[np.ndarray] = None,
    val_idx: Optional[np.ndarray] = None,
    test_idx: Optional[np.ndarray] = None,
) -> TokenizedCohort:
    """Load data and build tuple-token sequences: (Parameter|time_bin|value_bin).
    
    Expects wide-format parquet with columns like: Time, Parameter1, Parameter2, ..., PatientID, In-hospital_death
    
    Args:
        parquet_path: Path to parquet file
        n_value_bins: Number of quantile bins for value discretization
        max_seq_len: Maximum sequence length (tokens)
        train_idx: Training set indices (needed for scaling fit). If None, data is not imputed/scaled.
        val_idx: Validation set indices
        test_idx: Test set indices
    """
    # Load wide-format data
    full_df = pd.read_parquet(parquet_path)
    
    # Apply imputation and scaling if split indices are provided
    if train_idx is not None and val_idx is not None and test_idx is not None:
        full_df, _ = _impute_and_scale_data(full_df, train_idx, val_idx, test_idx)
    
    # Extract patient IDs and labels
    id_cols = ["PatientID", "In-hospital_death"]
    time_col = "Time"
    
    # All other columns are parameters (measurements over time)
    exclude_cols = id_cols + [time_col, "RecordID_x", "RecordID_y"]
    value_cols = [c for c in full_df.columns if c not in exclude_cols]

    # Compute global (dataset-level) quantile bins per parameter so value-bin tokens
    # have consistent semantics across patients.
    global_bin_edges: Dict[str, np.ndarray] = {}
    for param in value_cols:
        param_vals = full_df[param].dropna().values
        if len(param_vals) == 0:
            continue

        try:
            edges = np.quantile(param_vals, np.linspace(0, 1, n_value_bins + 1))
            edges[0] -= 1e-9
            edges[-1] += 1e-9
            # If all values are identical, quantiles collapse to one value.
            if np.allclose(edges, edges[0]):
                v = float(edges[0])
                edges = np.linspace(v - 1e-6, v + 1e-6, n_value_bins + 1)
        except Exception:
            vmin, vmax = float(np.min(param_vals)), float(np.max(param_vals))
            if np.isclose(vmin, vmax):
                edges = np.linspace(vmin - 1e-6, vmax + 1e-6, n_value_bins + 1)
            else:
                edges = np.linspace(vmin, vmax, n_value_bins + 1)

        global_bin_edges[param] = edges
    
    # Build vocabulary and tokenize
    vocab = {PAD_TOKEN: 0, UNK_TOKEN: 1, BOS_TOKEN: 2, EOS_TOKEN: 3}
    next_idx = 4

    all_input_ids = []
    patient_ids = []
    labels = []

    for patient_id, group in full_df.groupby("PatientID"):
        # Get label
        label_vals = group["In-hospital_death"].dropna().unique()
        if len(label_vals) == 0:
            continue
        label = int(label_vals[0])

        # Get time steps and values
        tokens = [vocab[BOS_TOKEN]]
        group = group.sort_values("Time")
        
        for param in value_cols:
            param_vals = group[param].dropna()
            if len(param_vals) == 0:
                continue

            bin_edges = global_bin_edges.get(param)
            if bin_edges is None:
                continue

            for time_idx, (val_idx, row) in enumerate(group[[param]].iterrows()):
                value = row[param]
                if pd.isna(value):
                    continue
                    
                param_str = str(param)
                time_bin = min(time_idx, 48)  # Cap at 48 hour steps
                value_bin = np.searchsorted(bin_edges, value, side="right") - 1
                value_bin = np.clip(value_bin, 0, n_value_bins - 1)

                token_str = f"{param_str}|t{time_bin}|v{value_bin}"
                if token_str not in vocab:
                    vocab[token_str] = next_idx
                    next_idx += 1
                tokens.append(vocab[token_str])

        tokens.append(vocab[EOS_TOKEN])
        tokens = tokens[:max_seq_len]

        all_input_ids.append(tokens)
        patient_ids.append(patient_id)
        labels.append(label)

    # Pad sequences
    padded_input_ids = []
    attention_masks = []
    for tokens in all_input_ids:
        mask = [1] * len(tokens) + [0] * (max_seq_len - len(tokens))
        tokens = tokens + [vocab[PAD_TOKEN]] * (max_seq_len - len(tokens))
        padded_input_ids.append(tokens)
        attention_masks.append(mask)

    return TokenizedCohort(
        input_ids=torch.LongTensor(padded_input_ids),
        attention_mask=torch.LongTensor(attention_masks),
        labels=torch.LongTensor(labels),
        patient_ids=patient_ids,
        vocab=vocab,
    )


# ============================================================================
# Main Q3 Pipeline
# ============================================================================

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Q3: Representation Learning for Medical Time Series")
    parser.add_argument("--parquet", type=str, default="processedDataProxy.parquet")
    parser.add_argument("--batch-size", type=int, default=64)
    parser.add_argument("--max-seq-len", type=int, default=1024)
    parser.add_argument("--value-bins", type=int, default=10)
    parser.add_argument("--d-model", type=int, default=128)
    parser.add_argument("--nhead", type=int, default=4)
    parser.add_argument("--layers", type=int, default=2)
    parser.add_argument("--pretrain-epochs", type=int, default=5, help="Q3.1: Pretraining epochs")
    parser.add_argument("--ssl-method", type=str, default="hybrid", choices=["autoencoder", "contrastive", "hybrid"])
    parser.add_argument("--mask-ratio", type=float, default=0.2)
    parser.add_argument("--drop-ratio", type=float, default=0.1)
    parser.add_argument("--temperature", type=float, default=0.1)
    parser.add_argument("--reconstruction-weight", type=float, default=1.0)
    parser.add_argument("--contrastive-weight", type=float, default=1.0)
    parser.add_argument("--multiscale-ratios", type=float, nargs="+", default=[1.0, 0.75, 0.5])
    parser.add_argument("--hard-negative-k", type=int, default=5)
    parser.add_argument("--time-freq-weight", type=float, default=0.1)
    parser.add_argument("--monitor-every", type=int, default=1)
    parser.add_argument("--early-stop-patience", type=int, default=5, help="Stop if val loss does not improve for N epochs (0 disables)")
    parser.add_argument("--early-stop-min-delta", type=float, default=1e-4, help="Minimum val loss improvement to reset patience")
    parser.add_argument(
        "--restore-best-weights",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Restore encoder weights from best val-loss epoch",
    )
    parser.add_argument("--tsne-perplexities", type=int, nargs="+", default=[20, 30, 40, 50])
    parser.add_argument("--umap-neighbors", type=int, nargs="+", default=[10, 15, 30, 50])
    parser.add_argument("--umap-min-dist", type=float, nargs="+", default=[0.05, 0.1, 0.25])
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--output-dir", type=str, default="./embeddings")
    parser.add_argument("--lr", type=float, default=1e-3)

    # Weights & Biases tracking / sweeps
    parser.add_argument("--use-wandb", action="store_true", help="Enable Weights & Biases logging")
    parser.add_argument("--wandb-project", type=str, default="ml4h-ssl")
    parser.add_argument("--wandb-entity", type=str, default=None)
    parser.add_argument("--wandb-run-name", type=str, default=None)
    parser.add_argument("--wandb-mode", type=str, default="online", choices=["online", "offline", "disabled"])
    parser.add_argument("--wandb-tags", type=str, nargs="*", default=[])
    parser.add_argument("--wandb-sweep-config", type=str, default=None, help="Path to W&B sweep config (json/yaml)")
    parser.add_argument("--wandb-sweep-count", type=int, default=10, help="Number of W&B sweep runs")
    return parser.parse_args()


def q31_pretrain_and_linear_probe(
    cohort: TokenizedCohort,
    train_idx: np.ndarray,
    val_idx: np.ndarray,
    test_idx: np.ndarray,
    device: torch.device,
    args: argparse.Namespace,
) -> Tuple[SequenceAutoEncoder, Dict]:
    """
    Q3.1: Pretrain encoder and evaluate with linear probe.
    
    Returns:
        - Pretrained encoder model
        - Results dict with metrics
    """
    print(f"\n{'='*70}")
    print("Q3.1: PRETRAINING AND LINEAR PROBES")
    print(f"{'='*70}")

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

    train_loader_generator = torch.Generator()
    train_loader_generator.manual_seed(args.seed)
    train_loader = DataLoader(
        train_dataset,
        batch_size=args.batch_size,
        shuffle=True,
        generator=train_loader_generator,
    )
    val_loader = DataLoader(val_dataset, batch_size=args.batch_size, shuffle=False)
    test_loader = DataLoader(test_dataset, batch_size=args.batch_size, shuffle=False)

    # Step 1: Pretrain encoder (selected SSL strategy)
    print(f"\nQ3.1.1: Pretraining encoder with ssl_method={args.ssl_method}...")
    model = SequenceAutoEncoder(
        vocab_size=len(cohort.vocab),
        d_model=args.d_model,
        nhead=args.nhead,
        num_layers=args.layers,
        max_len=cohort.input_ids.shape[1],
    )
    model.to(device)

    if args.ssl_method == "autoencoder":
        pretrainer: BaseSSLEncoder = AutoencoderPretrainer(
            model=model,
            pad_token_id=cohort.vocab[PAD_TOKEN],
            unk_token_id=cohort.vocab[UNK_TOKEN],
            bos_token_id=cohort.vocab[BOS_TOKEN],
            eos_token_id=cohort.vocab[EOS_TOKEN],
            mask_ratio=args.mask_ratio,
            drop_ratio=args.drop_ratio,
            monitor_every=args.monitor_every,
            early_stop_patience=args.early_stop_patience,
            early_stop_min_delta=args.early_stop_min_delta,
            restore_best_weights=args.restore_best_weights,
        )
    elif args.ssl_method == "contrastive":
        pretrainer = ContrastivePretrainer(
            model=model,
            pad_token_id=cohort.vocab[PAD_TOKEN],
            unk_token_id=cohort.vocab[UNK_TOKEN],
            bos_token_id=cohort.vocab[BOS_TOKEN],
            eos_token_id=cohort.vocab[EOS_TOKEN],
            mask_ratio=args.mask_ratio,
            drop_ratio=args.drop_ratio,
            temperature=args.temperature,
            multiscale_ratios=args.multiscale_ratios,
            hard_negative_k=args.hard_negative_k,
            time_freq_weight=args.time_freq_weight,
            monitor_every=args.monitor_every,
            early_stop_patience=args.early_stop_patience,
            early_stop_min_delta=args.early_stop_min_delta,
            restore_best_weights=args.restore_best_weights,
        )
    else:
        pretrainer = HybridPretrainer(
            model=model,
            pad_token_id=cohort.vocab[PAD_TOKEN],
            unk_token_id=cohort.vocab[UNK_TOKEN],
            bos_token_id=cohort.vocab[BOS_TOKEN],
            eos_token_id=cohort.vocab[EOS_TOKEN],
            mask_ratio=args.mask_ratio,
            drop_ratio=args.drop_ratio,
            temperature=args.temperature,
            reconstruction_weight=args.reconstruction_weight,
            contrastive_weight=args.contrastive_weight,
            multiscale_ratios=args.multiscale_ratios,
            hard_negative_k=args.hard_negative_k,
            time_freq_weight=args.time_freq_weight,
            monitor_every=args.monitor_every,
            early_stop_patience=args.early_stop_patience,
            early_stop_min_delta=args.early_stop_min_delta,
            restore_best_weights=args.restore_best_weights,
        )

    pretrainer.pretrain(train_loader, val_loader, device, args.pretrain_epochs, args.lr)
    monitoring = pretrainer.monitoring_metrics()
    if len(monitoring['train_losses']) > 0:
        print(f"Final train loss: {monitoring['train_losses'][-1]:.4f}")
        print(f"Final val loss: {monitoring['val_losses'][-1]:.4f}")
        if monitoring.get("best_epoch", 0) > 0:
            print(
                f"Best val loss: {monitoring['best_val_loss']:.4f} at epoch {monitoring['best_epoch']}"
                f" | Early stopped: {monitoring.get('early_stopped', False)}"
            )
    else:
        print("No pretraining epochs run (pretrain_epochs=0)")

    # Step 2: Extract embeddings and train linear probe
    print(f"\nQ3.1.2: Training linear probe on frozen encoder...")
    train_embeds, train_labels = get_embeddings(model, train_loader, device)
    test_embeds, test_labels = get_embeddings(model, test_loader, device)

    probe = LinearProbe()
    probe.train(train_embeds, train_labels)

    # Evaluate
    test_probs = probe.predict_proba(test_embeds)
    test_auroc, test_auprc = evaluate_metrics(test_labels, test_probs)

    rep_quality = ClusteringMetrics()
    rep_silhouette = rep_quality.silhouette_score(test_embeds, test_labels)
    rep_class_sep = rep_quality.compute_class_separation(test_embeds, test_labels)
    rep_centroid_dist = rep_class_sep.get("inter_class_distance", float("nan"))
    collapse_metrics = pretrainer._collapse_metrics(test_embeds)

    print(f"Test AUROC: {test_auroc:.4f}")
    print(f"Test AUPRC: {test_auprc:.4f}")
    print(f"Embedding Silhouette: {rep_silhouette:.4f}")
    print(f"Embedding Centroid Distance: {rep_centroid_dist:.4f}")
    print(
        "Collapse metrics: "
        f"std_mean={collapse_metrics['feature_std_mean']:.6f}, "
        f"std_min={collapse_metrics['feature_std_min']:.6f}, "
        f"active_dims={collapse_metrics['active_dims']:.0f}, "
        f"effective_rank={collapse_metrics['effective_rank']:.2f}, "
        f"mean_abs_cosine_offdiag={collapse_metrics['mean_abs_cosine_offdiag']:.4f}"
    )

    if monitoring.get("representation_monitor"):
        print("Per-epoch representation monitoring:")
        for m in monitoring["representation_monitor"]:
            print(
                f"  epoch={m['epoch']}: probe_auroc={m['probe_auroc']:.4f}, "
                f"silhouette={m['silhouette']:.4f}, centroid_distance={m['centroid_distance']:.4f}"
            )

    results_q31 = {
        "pretrain_method": args.ssl_method,
        "test_auroc": test_auroc,
        "test_auprc": test_auprc,
        "embedding_silhouette": rep_silhouette,
        "embedding_centroid_distance": rep_centroid_dist,
        "collapse_metrics": collapse_metrics,
        "pretrain_monitoring": monitoring,
        "embeddings_train": train_embeds,
        "labels_train": train_labels,
        "embeddings_test": test_embeds,
        "labels_test": test_labels,
    }

    return model, results_q31


def q32_label_scarcity(
    cohort: TokenizedCohort,
    train_idx: np.ndarray,
    test_idx: np.ndarray,
    model: SequenceAutoEncoder,
    device: torch.device,
    args: argparse.Namespace,
) -> Dict:
    """
    Q3.2: Simulate label scarcity. Compare pretrained vs supervised models.
    
    - Train supervised models with 100, 500, 1000 patients
    - Train linear probes with same subsets
    - Compare performance
    """
    print(f"\n{'='*70}")
    print("Q3.2: LABEL SCARCITY EXPERIMENTS")
    print(f"{'='*70}")

    # Fixed test set
    test_dataset = PatientTokenDataset(
        cohort.input_ids[test_idx],
        cohort.attention_mask[test_idx],
        cohort.labels[test_idx],
    )
    test_loader = DataLoader(test_dataset, batch_size=args.batch_size, shuffle=False)
    test_embeds, test_labels = get_embeddings(model, test_loader, device)

    results = []
    train_sizes = [100, 500, 1000]

    for train_size in train_sizes:
        print(f"\nTrain size: {train_size}")
        subset_idx = _stratified_subset_by_count(cohort.labels, train_idx, train_size, seed=args.seed)
        if len(np.intersect1d(subset_idx, test_idx)) > 0:
            raise ValueError("Data leakage detected: train subset overlaps with test set.")

        # Extract embeddings from subset
        subset_dataset = PatientTokenDataset(
            cohort.input_ids[subset_idx],
            cohort.attention_mask[subset_idx],
            cohort.labels[subset_idx],
        )
        subset_loader = DataLoader(subset_dataset, batch_size=args.batch_size, shuffle=False)
        subset_embeds, subset_labels = get_embeddings(model, subset_loader, device)

        # Q3.2.2: Linear probe on frozen embeddings (pretrained)
        probe = LinearProbe()
        probe.train(subset_embeds, subset_labels)
        probe_probs = probe.predict_proba(test_embeds)
        probe_auroc, probe_auprc = evaluate_metrics(test_labels, probe_probs)

        # Q3.2.1: TODO - Supervised model (currently placeholder with RF on embeddings)
        # In reality, should train supervised model from scratch on limited data
        rf = RandomForestModel()
        rf.train(subset_embeds, subset_labels)
        rf_probs = rf.predict_proba(test_embeds)
        supervised_auroc, supervised_auprc = evaluate_metrics(test_labels, rf_probs)

        results.append({
            "train_size": train_size,
            "supervised_auroc": supervised_auroc,
            "supervised_auprc": supervised_auprc,
            "pretrained_probe_auroc": probe_auroc,
            "pretrained_probe_auprc": probe_auprc,
        })

        print(f"  Supervised (RF): AUROC={supervised_auroc:.4f}, AUPRC={supervised_auprc:.4f}")
        print(f"  Pretrained+Probe: AUROC={probe_auroc:.4f}, AUPRC={probe_auprc:.4f}")

    return {"label_scarcity_results": results}


def q33_visualize_and_metrics(
    embeddings: np.ndarray,
    labels: np.ndarray,
    output_dir: str,
    tsne_perplexities: Optional[List[int]] = None,
    umap_neighbors: Optional[List[int]] = None,
    umap_min_dist: Optional[List[float]] = None,
) -> Dict:
    """
    Q3.3: Visualize representations and compute clustering metrics.
    
    - t-SNE and UMAP visualization
    - Silhouette score
    - Davies-Bouldin index
    - Class separation metrics
    """
    print(f"\n{'='*70}")
    print("Q3.3: VISUALIZING LEARNED REPRESENTATIONS")
    print(f"{'='*70}")

    visualizer = RepresentationVisualizer(
        output_dir=output_dir,
        tsne_perplexities=tsne_perplexities,
        umap_neighbors=umap_neighbors,
        umap_min_dist=umap_min_dist,
    )
    visualizer.visualize(embeddings, labels)

    # Compute clustering metrics
    print(f"\nQ3.3.2: Clustering Metrics")
    metrics = ClusteringMetrics()

    silhouette = metrics.silhouette_score(embeddings, labels)
    davies_bouldin = metrics.davies_bouldin_score(embeddings, labels)
    class_sep = metrics.compute_class_separation(embeddings, labels)

    print(f"Silhouette Score: {silhouette:.4f}")
    print(f"Davies-Bouldin Index: {davies_bouldin:.4f}")
    print("Class Separation Metrics:")
    for k, v in class_sep.items():
        print(f"  {k}: {v:.4f}")

    return {
        "silhouette_score": silhouette,
        "davies_bouldin_score": davies_bouldin,
        "class_separation_metrics": class_sep,
    }


def _namespace_to_config(args: argparse.Namespace) -> Dict:
    return {k: v for k, v in vars(args).items() if not k.startswith("wandb_") and k != "use_wandb"}


def _apply_wandb_config_overrides(args: argparse.Namespace, config_dict: Dict) -> None:
    for key, value in config_dict.items():
        if hasattr(args, key):
            setattr(args, key, value)


def _load_sweep_config(path: str) -> Dict:
    with open(path, "r", encoding="utf-8") as f:
        content = f.read()

    if path.endswith(".json"):
        return json.loads(content)
    if path.endswith(".yml") or path.endswith(".yaml"):
        if not YAML_AVAILABLE:
            raise ImportError("PyYAML is required for YAML sweep configs. Install with `pip install pyyaml`.")
        return yaml.safe_load(content)

    try:
        return json.loads(content)
    except json.JSONDecodeError:
        if not YAML_AVAILABLE:
            raise ValueError("Sweep config must be JSON or YAML (.json/.yml/.yaml).")
        return yaml.safe_load(content)


def _get_patient_level_labels_and_indices(parquet_path: str) -> Tuple[np.ndarray, np.ndarray]:
    """
    Extract one label per patient and patient count.
    
    Returns:
        - labels array (one per unique patient)
        - patient_ids array (corresponding patient IDs)
    """
    full_df = pd.read_parquet(parquet_path)
    
    # Get unique patients with their labels
    patient_labels = full_df.drop_duplicates(subset="PatientID")[["PatientID", "In-hospital_death"]]
    patient_labels = patient_labels.sort_values("PatientID").reset_index(drop=True)
    
    return patient_labels["In-hospital_death"].values, patient_labels["PatientID"].values


def _run_pipeline(args: argparse.Namespace) -> Dict:
    _set_global_reproducibility(args.seed)
    device = torch.device("cuda") if torch.cuda.is_available() else torch.device("cpu")
    print(f"Using device: {device}")

    # Step 1: Get patient labels and compute stratified split (before imputation/scaling)
    labels, _ = _get_patient_level_labels_and_indices(args.parquet)
    train_idx, val_idx, test_idx = _stratified_split_indices(
        labels,
        train_frac=0.7,
        val_frac=0.15,
        seed=args.seed,
    )
    _assert_disjoint_splits(train_idx, val_idx, test_idx)
    
    # Step 2: Load, impute, scale, and tokenize data
    cohort = build_tuple_token_dataset(
        parquet_path=args.parquet,
        n_value_bins=args.value_bins,
        max_seq_len=args.max_seq_len,
        train_idx=train_idx,
        val_idx=val_idx,
        test_idx=test_idx,
    )
    print(
        f"Tokenized cohort: patients={cohort.input_ids.shape[0]}, "
        f"seq_len={cohort.input_ids.shape[1]}, vocab={len(cohort.vocab)}"
    )

    # Q3.1: Pretrain and linear probe
    model, q31_results = q31_pretrain_and_linear_probe(cohort, train_idx, val_idx, test_idx, device, args)

    # Q3.2: Label scarcity
    q32_results = q32_label_scarcity(cohort, train_idx, test_idx, model, device, args)

    # Q3.3: Visualization and metrics
    q33_results = q33_visualize_and_metrics(
        q31_results["embeddings_test"],
        q31_results["labels_test"],
        output_dir=args.output_dir,
        tsne_perplexities=args.tsne_perplexities,
        umap_neighbors=args.umap_neighbors,
        umap_min_dist=args.umap_min_dist,
    )

    # Summary
    print(f"\n{'='*70}")
    print("SUMMARY")
    print(f"{'='*70}")
    print(f"Q3.1 SSL method: {q31_results['pretrain_method']}")
    print(f"Q3.1 Pretrained Linear Probe Test AUROC: {q31_results['test_auroc']:.4f}")
    print(f"Q3.1 Pretrained Linear Probe Test AUPRC: {q31_results['test_auprc']:.4f}")
    print(f"Q3.1 Embedding Silhouette: {q31_results['embedding_silhouette']:.4f}")
    print(f"Q3.1 Embedding Centroid Distance: {q31_results['embedding_centroid_distance']:.4f}")
    print(f"\nQ3.2 Label Scarcity Results:")
    for res in q32_results["label_scarcity_results"]:
        print(
            f"  n={res['train_size']}: "
            f"Supervised AUROC={res['supervised_auroc']:.4f}, "
            f"Pretrained AUROC={res['pretrained_probe_auroc']:.4f}"
        )
    print(f"\nQ3.3 Clustering Metrics:")
    print(f"  Silhouette: {q33_results['silhouette_score']:.4f}")
    print(f"  Davies-Bouldin: {q33_results['davies_bouldin_score']:.4f}")

    summary = {
        "summary/test_auroc": q31_results["test_auroc"],
        "summary/test_auprc": q31_results["test_auprc"],
        "summary/embedding_silhouette": q31_results["embedding_silhouette"],
        "summary/embedding_centroid_distance": q31_results["embedding_centroid_distance"],
        "summary/feature_std_mean": q31_results["collapse_metrics"]["feature_std_mean"],
        "summary/feature_std_min": q31_results["collapse_metrics"]["feature_std_min"],
        "summary/active_dims": q31_results["collapse_metrics"]["active_dims"],
        "summary/effective_rank": q31_results["collapse_metrics"]["effective_rank"],
        "summary/mean_abs_cosine_offdiag": q31_results["collapse_metrics"]["mean_abs_cosine_offdiag"],
        "summary/q33_silhouette": q33_results["silhouette_score"],
        "summary/q33_davies_bouldin": q33_results["davies_bouldin_score"],
    }

    for res in q32_results["label_scarcity_results"]:
        n = res["train_size"]
        summary[f"summary/scarcity_{n}_supervised_auroc"] = res["supervised_auroc"]
        summary[f"summary/scarcity_{n}_pretrained_auroc"] = res["pretrained_probe_auroc"]

    return {
        "q31": q31_results,
        "q32": q32_results,
        "q33": q33_results,
        "summary": summary,
    }


def _log_results_to_wandb(results: Dict) -> None:
    if not WANDB_AVAILABLE or wandb.run is None:
        return

    wandb.log(results["summary"])

    monitor = results["q31"].get("pretrain_monitoring", {}).get("representation_monitor", [])
    for m in monitor:
        step = int(m.get("epoch", 0))
        wandb.log(
            {
                "pretrain/probe_auroc": m.get("probe_auroc", float("nan")),
                "pretrain/silhouette": m.get("silhouette", float("nan")),
                "pretrain/centroid_distance": m.get("centroid_distance", float("nan")),
            },
            step=step,
        )


def _wandb_agent_run(base_args: argparse.Namespace) -> None:
    if not WANDB_AVAILABLE:
        raise ImportError("wandb is required for sweeps. Install with `pip install wandb`.")

    run = wandb.init(
        project=base_args.wandb_project,
        entity=base_args.wandb_entity,
        mode=base_args.wandb_mode,
        tags=base_args.wandb_tags,
        config=_namespace_to_config(base_args),
    )

    run_args = argparse.Namespace(**vars(base_args))
    _apply_wandb_config_overrides(run_args, dict(wandb.config))
    results = _run_pipeline(run_args)
    _log_results_to_wandb(results)
    wandb.finish()


def main() -> None:
    args = parse_args()

    if args.wandb_sweep_config is not None:
        if not WANDB_AVAILABLE:
            raise ImportError("wandb is required for sweeps. Install with `pip install wandb`.")
        sweep_config = _load_sweep_config(args.wandb_sweep_config)
        sweep_id = wandb.sweep(sweep=sweep_config, project=args.wandb_project, entity=args.wandb_entity)
        wandb.agent(sweep_id, function=lambda: _wandb_agent_run(args), count=args.wandb_sweep_count)
        return

    if args.use_wandb:
        if not WANDB_AVAILABLE:
            raise ImportError("wandb is required. Install with `pip install wandb`.")
        run = wandb.init(
            project=args.wandb_project,
            entity=args.wandb_entity,
            name=args.wandb_run_name,
            mode=args.wandb_mode,
            tags=args.wandb_tags,
            config=_namespace_to_config(args),
        )
        _apply_wandb_config_overrides(args, dict(wandb.config))
        results = _run_pipeline(args)
        _log_results_to_wandb(results)
        wandb.finish()
    else:
        _run_pipeline(args)


if __name__ == "__main__":
    main()
