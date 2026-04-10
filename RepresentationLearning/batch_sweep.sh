#!/usr/bin/env bash
#SBATCH --partition=jobs
#SBATCH --account=ml4h
#SBATCH --job-name=ml4h-sweep
#SBATCH --output=RepresentationLearning/logs/%x-%j.out
#SBATCH --error=RepresentationLearning/logs/%x-%j.err
#SBATCH --time=24:00:00

set -euo pipefail

# Always run from the project root (directory where sbatch was called).
cd "${SLURM_SUBMIT_DIR:-$PWD}"

mkdir -p RepresentationLearning/logs

# Create and activate a local virtual environment.
if [[ ! -d ".venv" ]]; then
  python3 -m venv .venv
fi
source .venv/bin/activate

python -m pip install --upgrade pip setuptools wheel

# Install dependencies from requirements if present and non-empty.
if [[ -s requirements.txt ]]; then
  python -m pip install -r requirements.txt
else
  # Fallback dependencies used by this project/sweep.
  python -m pip install \
    numpy pandas pyarrow tqdm matplotlib scikit-learn umap-learn pyyaml wandb torch
fi

# Build parquet dataset once if it doesn't exist.
if [[ ! -f "processedDataProxy.parquet" ]]; then
  echo "processedDataProxy.parquet not found -> running preprocessing"
  python RepresentationLearning/preprocessing_task3.py
fi

# W&B sweeps require online auth.
if [[ -z "${WANDB_API_KEY:-}" ]]; then
  echo "ERROR: WANDB_API_KEY is not set."
  echo "Set it before submission, e.g.: export WANDB_API_KEY=..."
  exit 1
fi

# Optional overrides via environment variables.
WANDB_PROJECT="ml4h_project"
WANDB_ENTITY="finn-brunke"
SWEEP_COUNT="${SWEEP_COUNT:-10}"

CMD=(
  python RepresentationLearning/auto_encoder_base.py
  --wandb-sweep-config RepresentationLearning/sweep_config.yaml
  --wandb-project "$WANDB_PROJECT"
  --wandb-sweep-count "$SWEEP_COUNT"
)

if [[ -n "$WANDB_ENTITY" ]]; then
  CMD+=(--wandb-entity "$WANDB_ENTITY")
fi

echo "Launching sweep with command: ${CMD[*]}"
"${CMD[@]}"
