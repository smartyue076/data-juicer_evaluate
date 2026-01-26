#!/usr/bin/env bash

# =========================
# 使用方式：
# ./run.sh 0 10
# 表示运行 000000.yaml ~ 000010.yaml
# =========================

START_IDX=$1
END_IDX=$2

if [ -z "$START_IDX" ] || [ -z "$END_IDX" ]; then
  echo "Usage: $0 <start_idx> <end_idx>"
  exit 1
fi

PIPELINE_DIR="/home/xuyue/cost_estimate/data-juicer_evaluate/config/text_pipeline"

for ((i=START_IDX; i<=END_IDX; i++)); do
  YAML_FILE=$(printf "%06d.yaml" "$i")
  YAML_PATH="${PIPELINE_DIR}/${YAML_FILE}"

  if [ ! -f "$YAML_PATH" ]; then
    echo "[WARN] ${YAML_PATH} not found, skip."
    continue
  fi

  echo "======================================"
  echo "[INFO] Running ${YAML_FILE}"
  echo "======================================"

  python /home/xuyue/cost_estimate/data-juicer/tools/process_data.py --config "$YAML_PATH"

  python remove_result.py

done
