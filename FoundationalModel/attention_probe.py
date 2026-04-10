import numpy as np
import pandas as pd
import torch
import torch.nn as nn
from torch.utils.data import DataLoader, TensorDataset
from sklearn.preprocessing import StandardScaler
from sklearn.model_selection import train_test_split
from sklearn.metrics import roc_auc_score, average_precision_score, brier_score_loss, log_loss
from datetime import datetime

# ── Config ───────────────────────────────────────────────────────────────────
N_CHANNELS  = 37
HIDDEN_DIM  = 512   # chronos-t5-base hidden dim
EPOCHS      = 50
BATCH_SIZE  = 64
LR          = 1e-3
DEVICE      = torch.device("cuda" if torch.cuda.is_available() else "cpu")
print(f"Using device: {DEVICE}")

# ── Load data ────────────────────────────────────────────────────────────────
ground_truth      = pd.read_csv("processedOutcomes-b.txt").set_index("RecordID")
ground_truth_test = pd.read_csv("processedOutcomes-c.txt").set_index("RecordID")
embeddings_df     = pd.read_csv("Embeddings/chronos.base_embeddings.csv").set_index("RecordID")
embeddings_test   = pd.read_csv("Embeddings/chronos.base_embeddings_test.csv").set_index("RecordID")

def prepare(embeddings_df, ground_truth):
    merged = ground_truth[["In-hospital_death"]].join(embeddings_df).dropna()
    X = merged.drop(columns=["In-hospital_death"]).values
    y = merged["In-hospital_death"].values
    return X, y

X, y          = prepare(embeddings_df,   ground_truth)
X_test, y_test = prepare(embeddings_test, ground_truth_test)

# Scale
scaler = StandardScaler()
X      = scaler.fit_transform(X)
X_test = scaler.transform(X_test)

# Train/val split
X_train, X_val, y_train, y_val = train_test_split(X, y, test_size=0.2, random_state=42)

# Reshape to (batch, n_channels, hidden_dim)
def to_channels(X):
    return torch.tensor(
        X.reshape(-1, N_CHANNELS, HIDDEN_DIM), dtype=torch.float32
    )

X_train_t = to_channels(X_train)
X_val_t   = to_channels(X_val)
X_test_t  = to_channels(X_test)
y_train_t = torch.tensor(y_train, dtype=torch.float32)
y_val_t   = torch.tensor(y_val,   dtype=torch.float32)
y_test_t  = torch.tensor(y_test,  dtype=torch.float32)

train_loader = DataLoader(
    TensorDataset(X_train_t, y_train_t),
    batch_size=BATCH_SIZE, shuffle=True
)

# ── Architecture ─────────────────────────────────────────────────────────────
class ChannelAttentionProbe(nn.Module):
    """
    Lightweight architecture that learns to aggregate channel embeddings.
    
    Instead of averaging 37 channel embeddings equally, an attention
    mechanism learns which channels are most predictive of mortality.
    
    Input:  (batch, n_channels, hidden_dim)
    Output: (batch, 1) — mortality probability
    """
    def __init__(self, n_channels, hidden_dim, mlp_dim=64):
        super().__init__()

        # Attention: learns a scalar weight per channel
        # "How important is this channel for mortality prediction?"
        self.attention = nn.Sequential(
            nn.Linear(hidden_dim, 32),
            nn.Tanh(),
            nn.Linear(32, 1)           # scalar score per channel
        )

        # Small MLP classifier on the aggregated embedding
        self.classifier = nn.Sequential(
            nn.Linear(hidden_dim, mlp_dim),
            nn.ReLU(),
            nn.Dropout(0.3),
            nn.Linear(mlp_dim, 1)
        )

    def forward(self, x):
        # x: (batch, n_channels, hidden_dim)

        # Compute attention weights over channels
        scores  = self.attention(x)              # (batch, n_channels, 1)
        weights = torch.softmax(scores, dim=1)   # (batch, n_channels, 1) — sum to 1

        # Weighted sum across channels
        aggregated = (weights * x).sum(dim=1)    # (batch, hidden_dim)

        # Classify
        out = self.classifier(aggregated)        # (batch, 1)
        return out.squeeze(1)                    # (batch,)


# ── Training ─────────────────────────────────────────────────────────────────
model     = ChannelAttentionProbe(N_CHANNELS, HIDDEN_DIM).to(DEVICE)
optimizer = torch.optim.Adam(model.parameters(), lr=LR, weight_decay=1e-4)

# Class weights for imbalanced data
pos_weight = torch.tensor([(y_train == 0).sum() / (y_train == 1).sum()]).to(DEVICE)
criterion  = nn.BCEWithLogitsLoss(pos_weight=pos_weight)

print(f"\nTraining ChannelAttentionProbe for {EPOCHS} epochs...")
best_val_auc = 0
best_state   = None

for epoch in range(EPOCHS):
    # Train
    model.train()
    for X_batch, y_batch in train_loader:
        X_batch, y_batch = X_batch.to(DEVICE), y_batch.to(DEVICE)
        optimizer.zero_grad()
        loss = criterion(model(X_batch), y_batch)
        loss.backward()
        optimizer.step()

    # Validate
    model.eval()
    with torch.no_grad():
        val_logits = model(X_val_t.to(DEVICE)).cpu().numpy()
        val_probs  = torch.sigmoid(torch.tensor(val_logits)).numpy()
        val_auc    = roc_auc_score(y_val, val_probs)

    if val_auc > best_val_auc:
        best_val_auc = val_auc
        best_state   = model.state_dict().copy()

    if (epoch + 1) % 10 == 0:
        print(f"Epoch {epoch+1}/{EPOCHS} — Val AUC: {val_auc:.3f} (best: {best_val_auc:.3f})")

# ── Test evaluation ───────────────────────────────────────────────────────────
model.load_state_dict(best_state)
model.eval()

with torch.no_grad():
    test_logits = model(X_test_t.to(DEVICE)).cpu().numpy()
    y_prob      = torch.sigmoid(torch.tensor(test_logits)).numpy()

auc     = roc_auc_score(y_test, y_prob)
auprc   = average_precision_score(y_test, y_prob)
brier   = brier_score_loss(y_test, y_prob)
logloss = log_loss(y_test, y_prob)

print(f"\n── Test Results ──────────────────────────────────────")
print(f"AuROC:    {auc:.3f}")
print(f"AUPRC:    {auprc:.3f}")
print(f"Brier:    {brier:.3f}")
print(f"LogLoss:  {logloss:.3f}")

with open("attention_probe_results.txt", "a") as f:
    f.write(f"\n--- Run {datetime.now().strftime('%Y%m%d_%H%M%S')} ---\n")
    f.write(f"AuROC={auc:.3f} | AUPRC={auprc:.3f} | Brier={brier:.3f} | LogLoss={logloss:.3f}\n")

# ── Inspect learned attention weights ────────────────────────────────────────
# Which channels did the model learn to focus on?
model.eval()
with torch.no_grad():
    dummy    = X_test_t[:1].to(DEVICE)
    scores   = model.attention(dummy)              # (1, n_channels, 1)
    weights  = torch.softmax(scores, dim=1).squeeze().cpu().numpy()

feature_cols = [c for c in embeddings_df.columns if c != "RecordID"]
channel_names = feature_cols[:N_CHANNELS] if len(feature_cols) >= N_CHANNELS else [f"ch_{i}" for i in range(N_CHANNELS)]

print(f"\n── Top 10 most attended channels ────────────────────")
top_idx = np.argsort(weights)[::-1][:10]
for i in top_idx:
    print(f"  Channel {i:2d} ({channel_names[i] if i < len(channel_names) else '?'}): {weights[i]:.4f}")