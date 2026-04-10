#!/usr/bin/env bash
#SBATCH --partition=jobs
#SBATCH --account=ml4h
#SBATCH --job-name=ml4h-label-scarcity
#SBATCH --output=RepresentationLearning/logs/%x-%j.out
#SBATCH --error=RepresentationLearning/logs/%x-%j.err
#SBATCH --time=24:00:00

set -euo pipefail

# Reproducibility settings
export PYTHONHASHSEED=42
export CUBLAS_WORKSPACE_CONFIG=:4096:8
export PYTHONUNBUFFERED=1

# Always run from submission directory
cd "${SLURM_SUBMIT_DIR:-$PWD}"

mkdir -p "RepresentationLearning/logs"
mkdir -p "RepresentationLearning"

# Create / activate local venv
if [[ ! -d ".venv" ]]; then
  python3 -m venv .venv
fi
source .venv/bin/activate

python -m pip install --upgrade pip setuptools wheel

# Install dependencies
if [[ -s requirements.txt ]]; then
  if ! python -m pip install -r requirements.txt; then
    echo "requirements.txt install failed; falling back to minimal label-scarcity dependencies"
    python -m pip install numpy pandas pyarrow tqdm matplotlib scikit-learn umap-learn pyyaml torch
  fi
else
  python -m pip install numpy pandas pyarrow tqdm matplotlib scikit-learn umap-learn pyyaml torch
fi

# Build parquet once if missing
if [[ ! -f "processedDataProxy.parquet" ]]; then
  echo "processedDataProxy.parquet not found -> running preprocessing"
  python -u RepresentationLearning/preprocessing_task3.py
fi

# Optional overrides
EPOCHS="${EPOCHS:-10}"
BATCH_SIZE="${BATCH_SIZE:-32}"
LR="${LR:-0.001}"
SEED="${SEED:-42}"

CMD=(
  python -u RepresentationLearning/label_scarcity.py
  --parquet "processedDataProxy.parquet"
  --seed "$SEED"
  --epochs "$EPOCHS"
  --batch-size "$BATCH_SIZE"
  --lr "$LR"
  --output-csv "RepresentationLearning/label_scarcity_results.csv"
)

echo "Launching label scarcity benchmark: ${CMD[*]}"
"${CMD[@]}"
