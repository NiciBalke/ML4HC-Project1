#!/usr/bin/env bash
#SBATCH --partition=jobs
#SBATCH --account=ml4h
#SBATCH --job-name=ml4h-best-contrastive
#SBATCH --output=RepresentationLearning/logs/%x-%j.out
#SBATCH --error=RepresentationLearning/logs/%x-%j.err
#SBATCH --time=24:00:00

set -euo pipefail

# Reproducibility settings (must be set before Python starts)
export PYTHONHASHSEED=42
export CUBLAS_WORKSPACE_CONFIG=:4096:8

# Always run from the project root (directory where sbatch was called)
cd "${SLURM_SUBMIT_DIR:-$PWD}"

mkdir -p RepresentationLearning/logs

# Create / activate local virtual environment
if [[ ! -d ".venv" ]]; then
  python3 -m venv .venv
fi
source .venv/bin/activate

python -m pip install --upgrade pip setuptools wheel

# Install dependencies
if [[ -s requirements.txt ]]; then
  python -m pip install -r requirements.txt
else
  python -m pip install \
    numpy pandas pyarrow tqdm matplotlib scikit-learn umap-learn pyyaml wandb torch
fi

# Build parquet once if missing
if [[ ! -f "processedDataProxy.parquet" ]]; then
  echo "processedDataProxy.parquet not found -> running preprocessing"
  python RepresentationLearning/preprocessing_task3.py
fi

# Optional W&B tracking (disabled by default)
USE_WANDB="${USE_WANDB:-0}"
WANDB_PROJECT="${WANDB_PROJECT:-ml4h_project}"
WANDB_ENTITY="${WANDB_ENTITY:-finn-brunke}"

OUTPUT_DIR="${OUTPUT_DIR:-RepresentationLearning/embeddings/best_contrastive_run}"
mkdir -p "$OUTPUT_DIR"

CMD=(
  python RepresentationLearning/auto_encoder_base.py
  --parquet processedDataProxy.parquet
  --batch-size 16
  --d-model 64
  --nhead 2
  --layers 3
  --pretrain-epochs 30
  --ssl-method contrastive
  --lr 0.0005
  --mask-ratio 0.1
  --drop-ratio 0.05
  --temperature 0.05
  --reconstruction-weight 1.5
  --contrastive-weight 1.5
  --multiscale-ratios 1.0
  --hard-negative-k 3
  --time-freq-weight 0.0
  --monitor-every 1
  --max-seq-len 512
  --value-bins 10
  --seed 42
  --early-stop-patience 5
  --early-stop-min-delta 0.0001
  --restore-best-weights
  --output-dir "$OUTPUT_DIR"
)

if [[ "$USE_WANDB" == "1" ]]; then
  if [[ -z "${WANDB_API_KEY:-}" ]]; then
    echo "ERROR: USE_WANDB=1 but WANDB_API_KEY is not set."
    exit 1
  fi
  CMD+=(--use-wandb --wandb-project "$WANDB_PROJECT")
  if [[ -n "$WANDB_ENTITY" ]]; then
    CMD+=(--wandb-entity "$WANDB_ENTITY")
  fi
fi

echo "Launching best-contrastive run: ${CMD[*]}"
"${CMD[@]}"
