import numpy as np
import pandas as pd
from sklearn.linear_model import LogisticRegression
from sklearn.preprocessing import StandardScaler
from sklearn.model_selection import train_test_split
from sklearn.metrics import (
    roc_auc_score,
    brier_score_loss,
    log_loss,
    average_precision_score
)
from datetime import datetime

# ── Train data ──────────────────────────────────────────────────────────────
ground_truth  = pd.read_csv("processedOutcomes-b.txt").set_index("RecordID")
embeddings_df = pd.read_csv("Embeddings/chronos.base_embeddings.csv").set_index("RecordID")

merged = ground_truth[["In-hospital_death"]].join(embeddings_df).dropna()
X = merged.drop(columns=["In-hospital_death"]).values
y = merged["In-hospital_death"].values

# Split train into train/val to pick best C
X_train, X_val, y_train, y_val = train_test_split(X, y, test_size=0.2, random_state=42)

scaler = StandardScaler()
X_train = scaler.fit_transform(X_train)
X_val   = scaler.transform(X_val)

# ── Test data ────────────────────────────────────────────────────────────────
# TODO: replace with chronos embeddings of test set once generated
ground_truth_test  = pd.read_csv("processedOutcomes-c.txt").set_index("RecordID")
embeddings_test_df = pd.read_csv("Embeddings/chronos.base_embeddings_test.csv").set_index("RecordID")

merged_test = ground_truth_test[["In-hospital_death"]].join(embeddings_test_df).dropna()
X_test_final = merged_test.drop(columns=["In-hospital_death"]).values
y_test_final = merged_test["In-hospital_death"].values
X_test_final = scaler.transform(X_test_final)  # use same scaler fitted on train

# ── Validation loop to find best C ──────────────────────────────────────────
with open("probe_results.txt", "a") as f:
    f.write(f"\n--- Run {datetime.now().strftime('%Y%m%d_%H%M%S')} ---\n")

best_auc = 0
best_C   = None
best_clf = None

for C in [0.001, 0.01, 0.1, 1.0]:
    clf = LogisticRegression(C=C, max_iter=1000, random_state=42)
    clf.fit(X_train, y_train)

    y_prob  = clf.predict_proba(X_val)[:, 1]
    auc     = roc_auc_score(y_val, y_prob)
    auprc   = average_precision_score(y_val, y_prob)
    brier   = brier_score_loss(y_val, y_prob)
    logloss = log_loss(y_val, y_prob)

    print(f"[VAL] C={C}: AuROC={auc:.3f} | AUPRC={auprc:.3f} | Brier={brier:.3f} | LogLoss={logloss:.3f}")
    with open("probe_results.txt", "a") as f:
        f.write(f"[VAL] C={C}: AuROC={auc:.3f} | AUPRC={auprc:.3f} | Brier={brier:.3f} | LogLoss={logloss:.3f}\n")

    if auc > best_auc:
        best_auc = auc
        best_C   = C
        best_clf = clf

# ── Evaluate best model on held-out test set ────────────────────────────────
print(f"\nBest C={best_C} (val AUC={best_auc:.3f}), evaluating on test set...")

y_prob_test = best_clf.predict_proba(X_test_final)[:, 1]
auc_test     = roc_auc_score(y_test_final, y_prob_test)
auprc_test   = average_precision_score(y_test_final, y_prob_test)
brier_test   = brier_score_loss(y_test_final, y_prob_test)
logloss_test = log_loss(y_test_final, y_prob_test)

print(f"[TEST] C={best_C}: AuROC={auc_test:.3f} | AUPRC={auprc_test:.3f} | Brier={brier_test:.3f} | LogLoss={logloss_test:.3f}")
with open("probe_results.txt", "a") as f:
    f.write(f"\n[TEST] best C={best_C}: AuROC={auc_test:.3f} | AUPRC={auprc_test:.3f} | Brier={brier_test:.3f} | LogLoss={logloss_test:.3f}\n")