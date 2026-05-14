#!/usr/bin/env bash
# IMMUTABLE. One-time SIFT1M fixture preparation.
#
# Downloads SIFT1M from the Texmex corpus (Inria), extracts the f32 vector
# files + ground-truth, then runs the in-tree fixture builder to train a
# product-quantization codebook and encode the base set. All artifacts are
# written under ~/.cache/lance-autoresearch/ so they survive between trials
# but stay out of git.
#
# Total time: ~5–10 min on a fresh laptop. ~250 MB download.

set -euo pipefail

CACHE_DIR="${HOME}/.cache/lance-autoresearch"
SIFT_URL="ftp://ftp.irisa.fr/local/texmex/corpus/sift.tar.gz"
SIFT_URL_MIRROR="https://huggingface.co/datasets/qbo-odp/sift1m/resolve/main/sift.tar.gz"

mkdir -p "${CACHE_DIR}"
cd "${CACHE_DIR}"

if [[ ! -f sift_base.fvecs || ! -f sift_query.fvecs || ! -f sift_groundtruth.ivecs ]]; then
  echo "[prepare_fixtures] downloading SIFT1M..."
  if [[ ! -f sift.tar.gz ]]; then
    curl --fail -L -o sift.tar.gz "${SIFT_URL}" || \
      curl --fail -L -o sift.tar.gz "${SIFT_URL_MIRROR}"
  fi
  echo "[prepare_fixtures] extracting..."
  tar xzf sift.tar.gz
  mv sift/sift_base.fvecs        ./sift_base.fvecs
  mv sift/sift_query.fvecs       ./sift_query.fvecs
  mv sift/sift_groundtruth.ivecs ./sift_groundtruth.ivecs
  rm -rf sift sift.tar.gz
fi

if [[ ! -f pq_codebook.bin || ! -f pq_codes.bin ]]; then
  echo "[prepare_fixtures] training PQ codebook + encoding base..."
  # The fixture builder is run as a `cargo test` with a marker env var so we
  # don't have to add a second binary just for one-time setup. The test reads
  # SIFT1M, calls the in-tree `train_codebook` + `encode`, and writes the
  # frozen artifacts next to the dataset.
  cd "$(dirname "$0")/.."
  LANCE_AUTORESEARCH_BUILD_FIXTURES=1 cargo test --release --lib build_fixtures -- --ignored --nocapture
fi

echo "[prepare_fixtures] done — fixtures in ${CACHE_DIR}"
ls -la "${CACHE_DIR}"
