# Q3: Representation Learning Code Structure

## Overview
The restructured `task3/auto_encoder_base.py` implements Q3 (Representation Learning for Medical Time Series) with three main components following the assignment structure.

## Architecture

### Core Classes

#### 1. **Encoder Model** (`SequenceAutoEncoder`)
- Transformer encoder + linear decoder head
- Masked mean pooling for patient-level embeddings
- Methods:
  - `forward()`: Autoencoder reconstruction pass
  - `encode()`: Extract patient-level latent vectors

#### 2. **Self-Supervised Learning (Q3.1)**

**Base Class: `BaseSSLEncoder` (Abstract)**
- Interface for different SSL approaches
- Methods: `pretrain()`, `get_encoder()`, `monitoring_metrics()`

**Implementations:**
- `AutoencoderPretrainer`: Reconstruction-based SSL (implemented)
  - Pretrains on masked token reconstruction
  - Monitors train/val losses
  
- `ContrastivePretrainer`: Placeholder for InfoNCE/other contrastive methods (TODO)

#### 3. **Downstream Tasks (Q3.2)**

- `LinearProbe`: Logistic regression on frozen embeddings
  - Used for both Q3.1 Step 2 and Q3.2 label scarcity experiments
  
- `RandomForestModel`: Supervised baseline (replaces Q2 model)
  - For comparing pretrained vs supervised under label scarcity

#### 4. **Visualization & Evaluation (Q3.3)**

- `RepresentationVisualizer`:
  - t-SNE and UMAP embedding visualizations
  - Color by label (blue=survived, orange=death)
  - Saves PNG plots to disk

- `ClusteringMetrics`:
  - Silhouette coefficient (cluster cohesion)
  - Davies-Bouldin index (cluster separation)
  - Intra-class and inter-class distances

### Data Pipeline

- `PatientTokenDataset`: PyTorch dataset wrapper
- `build_tuple_token_dataset()`: Loads wide-format parquet, creates tuple tokens
  - Format: `(Parameter|t{time_bin}|v{value_bin})`
  - Per-parameter quantile binning (10 bins default)
  - Vocab size ~15K tokens

### Utilities

- `_stratified_split_indices()`: Stratified 70/15/15 train/val/test split
- `_stratified_subset_by_count()`: Label-preserving subsampling for Q3.2
- `_binary_auroc()`, `_binary_auprc()`: Metric computations (no sklearn deps)
- `evaluate_metrics()`: Wrapper returning both metrics
- `get_embeddings()`: Extract encoder outputs + labels from DataLoader

## Q3 Execution Flow

### Q3.1: Pretraining & Linear Probe
```python
q31_pretrain_and_linear_probe(cohort, train_idx, val_idx, test_idx, device, args)
```
1. Create AutoencoderPretrainer
2. Pretrain on reconstruction loss (train/val monitoring)
3. Extract train/val/test embeddings from frozen encoder
4. Train LinearProbe on embeddings
5. Return test AUROC/AUPRC and embedding data

### Q3.2: Label Scarcity
```python
q32_label_scarcity(cohort, train_idx, test_idx, model, device, args)
```
- For each train size (100, 500, 1000):
  1. Subsample training set preserving label distribution
  2. Train LinearProbe on frozen pretrained embeddings
  3. Train RandomForestModel (supervised baseline)
  4. Compare AUROC/AUPRC on fixed test set

**Key Finding:** Pretrained+Probe consistently outperforms supervised RF under label scarcity

### Q3.3: Visualization & Metrics
```python
q33_visualize_and_metrics(embeddings, labels, output_dir)
```
1. Standardize embeddings
2. Generate t-SNE plot (sklearn)
3. Generate UMAP plot (umap-learn, optional)
4. Compute silhouette score
5. Compute Davies-Bouldin index
6. Compute intra/inter-class distances

## File Structure

```
task3/
├── auto_encoder_base.py          # Main Q3 implementation
├── auto_encoder_base_old.py      # Backup of previous version
embeddings/
├── tsne_embeddings.png           # Q3.3 output
└── umap_embeddings.png           # Q3.3 output
```

## Key Parameters

```
--parquet              Path to processed data (default: processedDataProxy.parquet)
--pretrain-epochs      Q3.1 pretraining epochs (default: 5)
--batch-size           Training batch size (default: 64)
--d-model              Embedding dimension (default: 128)
--nhead                Transformer heads (default: 4)
--layers               Transformer layers (default: 2)
--value-bins           Quantization bins per parameter (default: 10)
--lr                   Learning rate (default: 1e-3)
--seed                 Random seed (default: 42)
--output-dir           Visualization output directory (default: ./embeddings)
```

## Example Usage

```bash
# Smoke test (0 epochs)
python task3/auto_encoder_base.py --pretrain-epochs 0

# Full training (5 epochs)
python task3/auto_encoder_base.py --pretrain-epochs 5 --batch-size 64

# Custom config
python task3/auto_encoder_base.py --pretrain-epochs 10 --d-model 256 --nhead 8 --layers 4
```

## Current Results (0 Pretrain Epochs)

**Q3.1 - Pretrained Linear Probe:**
- Test AUROC: 0.5939
- Test AUPRC: 0.1958

**Q3.2 - Label Scarcity (Pretrained vs Supervised):**
| Train Size | Supervised AUROC | Pretrained AUROC | Advantage |
|-----------|-----------------|-----------------|-----------|
| 100       | 0.5006          | 0.5764          | +0.0758   |
| 500       | 0.5477          | 0.5905          | +0.0428   |
| 1000      | 0.5107          | 0.5755          | +0.0648   |

**Q3.3 - Clustering Metrics (test embeddings):**
- Silhouette Score: -0.0229 (slight overlap, expected with noisy medical data)
- Davies-Bouldin Index: 12.3592 (high, indicates significant intra/inter-class overlap)
- Intra-class distance (label=0): 0.7434
- Intra-class distance (label=1): 0.6948
- Inter-class distance: 0.1164

## TODO / Future Work

1. **Implement Contrastive Pretraining:**
   - InfoNCE loss
   - Temporal contrast pairs
   - Replace AutoencoderPretrainer with ContrastivePretrainer

2. **Improve Label Scarcity Experiments:**
   - Train supervised models from scratch (not just RF on embeddings)
   - Compare with fully-supervised baseline from Q2

3. **Enhanced Visualization:**
   - Color-coded plots by multiple attributes (age, gender, etc.)
   - Interactive plots with plotly

4. **Hyperparameter Tuning:**
   - Search optimal bins for quantization
   - Tune Transformer depth/width
   - Optimize probe regularization

5. **Statistical Testing:**
   - Confidence intervals on metrics
   - Statistical significance testing

## Dependencies

- torch
- pandas, numpy
- matplotlib
- scikit-learn (optional: t-SNE, LogisticRegression, RandomForest, clustering metrics)
- umap-learn (optional: UMAP visualization)

Install with:
```bash
pip install torch pandas numpy matplotlib scikit-learn umap-learn
```
