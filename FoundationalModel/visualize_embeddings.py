import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from sklearn.preprocessing import StandardScaler
from sklearn.decomposition import PCA
from sklearn.manifold import TSNE
from umap import UMAP

# ── Load embeddings and labels ───────────────────────────────────────────────
ground_truth = pd.read_csv("processedOutcomes-b.txt").set_index("RecordID")

# Load both embedding types
llm_df     = pd.read_csv("Embeddings/llm_embeddings.csv").set_index("RecordID")
chronos_df = pd.read_csv("Embeddings/chronos.base_embeddings.csv").set_index("RecordID")

def prepare(embeddings_df, ground_truth):
    merged = ground_truth[["In-hospital_death"]].join(embeddings_df).dropna()
    X = merged.drop(columns=["In-hospital_death"]).values
    y = merged["In-hospital_death"].values
    X = StandardScaler().fit_transform(X)
    return X, y

X_llm,     y_llm     = prepare(llm_df,     ground_truth)
X_chronos, y_chronos = prepare(chronos_df, ground_truth)

# ── Dimensionality reduction ─────────────────────────────────────────────────
def reduce_all(X):
    print("  Running PCA...")
    pca   = PCA(n_components=2, random_state=42).fit_transform(X)
    print("  Running t-SNE...")
    tsne  = TSNE(n_components=2, random_state=42, perplexity=30).fit_transform(X)
    print("  Running UMAP...")
    umap  = UMAP(n_components=2, random_state=42).fit_transform(X)
    return pca, tsne, umap

print("Reducing LLM embeddings...")
pca_llm, tsne_llm, umap_llm = reduce_all(X_llm)

print("Reducing Chronos embeddings...")
pca_chr, tsne_chr, umap_chr = reduce_all(X_chronos)

# ── Plot ─────────────────────────────────────────────────────────────────────
fig, axes = plt.subplots(2, 3, figsize=(18, 10))

titles  = ["PCA", "t-SNE", "UMAP"]
llm_red = [pca_llm,  tsne_llm,  umap_llm]
chr_red = [pca_chr,  tsne_chr,  umap_chr]

colors  = {0: "#4C72B0", 1: "#DD8452"}
labels  = {0: "Survived", 1: "Died"}

def scatter(ax, coords, y, title, row_label):
    for outcome in [0, 1]:
        mask = y == outcome
        ax.scatter(
            coords[mask, 0], coords[mask, 1],
            c=colors[outcome], label=labels[outcome],
            alpha=0.4, s=10, linewidths=0
        )
    ax.set_title(f"{row_label} — {title}", fontsize=12)
    ax.set_xticks([])
    ax.set_yticks([])
    ax.legend(fontsize=8, markerscale=2)

for i, (title, llm, chron) in enumerate(zip(titles, llm_red, chr_red)):
    scatter(axes[0, i], llm,  y_llm,     title, "LLM")
    scatter(axes[1, i], chron, y_chronos, title, "Chronos")

plt.suptitle("Embedding Visualizations: LLM vs Chronos", fontsize=14, fontweight="bold")
plt.tight_layout()
plt.savefig("embedding_comparison.png", dpi=150, bbox_inches="tight")
plt.show()
print("Saved to embedding_comparison.png")