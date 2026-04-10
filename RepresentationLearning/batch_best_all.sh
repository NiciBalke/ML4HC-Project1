#!/usr/bin/env bash
set -euo pipefail

# Submit all three best-config jobs.
# Usage: bash RepresentationLearning/batch_best_all.sh

cd "${SLURM_SUBMIT_DIR:-$PWD}"

jid_auto=$(sbatch RepresentationLearning/batch_best_auto.sh | awk '{print $4}')
jid_contrastive=$(sbatch RepresentationLearning/batch_best_contrastive.sh | awk '{print $4}')
jid_hybrid=$(sbatch RepresentationLearning/batch_best_hybrid.sh | awk '{print $4}')

echo "Submitted jobs:"
echo "  autoencoder : ${jid_auto}"
echo "  contrastive : ${jid_contrastive}"
echo "  hybrid      : ${jid_hybrid}"
