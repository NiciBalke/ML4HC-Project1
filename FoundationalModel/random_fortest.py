import numpy as np
import pandas as pd
from sklearn.ensemble import RandomForestClassifier
from sklearn.preprocessing import StandardScaler
from sklearn.model_selection import train_test_split
from sklearn.metrics import (
    roc_auc_score,
    brier_score_loss,
    log_loss,
    average_precision_score
)
from datetime import datetime


print("predicting with random forest")


ground_truth  = pd.read_csv("processedOutcomes-b.txt").set_index("RecordID")
embeddings_df = pd.read_csv("Embeddings/chronos.base_embeddings.csv").set_index("RecordID")

merged = ground_truth[["In-hospital_death"]].join(embeddings_df).dropna()

X = merged.drop(columns=["In-hospital_death"]).values
y = merged["In-hospital_death"].values

X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

# Random forest doesn't need scaling but kept for consistency
scaler = StandardScaler()
X_train = scaler.fit_transform(X_train)
X_test  = scaler.transform(X_test)

with open("rf_results.txt", "a") as f:
    f.write(f"\n--- Run {datetime.now().strftime('%Y%m%d_%H%M%S')} ---\n")

for n_estimators, max_depth in [(100, None), (100, 5), (200, None), (200, 10)]:
    clf = RandomForestClassifier(
        n_estimators=n_estimators,
        max_depth=max_depth,
        class_weight="balanced",  # handles class imbalance
        random_state=42,
        n_jobs=-1                 # use all CPU cores
    )
    clf.fit(X_train, y_train)

    y_prob = clf.predict_proba(X_test)[:, 1]

    auc     = roc_auc_score(y_test, y_prob)
    auprc   = average_precision_score(y_test, y_prob)
    brier   = brier_score_loss(y_test, y_prob)
    logloss = log_loss(y_test, y_prob)

    print(f"n_estimators={n_estimators}, max_depth={max_depth}: AuROC={auc:.3f} | AUPRC={auprc:.3f} | Brier={brier:.3f} | LogLoss={logloss:.3f}")
    with open("rf_results.txt", "a") as f:
        f.write(f"n_estimators={n_estimators}, max_depth={max_depth}: AuROC={auc:.3f} | AUPRC={auprc:.3f} | Brier={brier:.3f} | LogLoss={logloss:.3f}\n")