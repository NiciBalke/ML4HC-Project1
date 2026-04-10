#!/usr/bin/env bash
set -euo pipefail

# Submit all three best-config jobs.
# Usage: bash task3/batch_best_all.sh

cd "${SLURM_SUBMIT_DIR:-$PWD}"

jid_auto=$(sbatch task3/batch_best.sh | awk '{print $4}')
jid_contrastive=$(sbatch task3/batch_best_contrastive.sh | awk '{print $4}')
jid_hybrid=$(sbatch task3/batch_best_hybrid.sh | awk '{print $4}')

echo "Submitted jobs:"
echo "  autoencoder : ${jid_auto}"
echo "  contrastive : ${jid_contrastive}"
echo "  hybrid      : ${jid_hybrid}"
