
#!/bin/bash
set -e

python src/generate_data.py --scale 1

python src/pipeline.py \
    --input data/input \
    --output output \
    --valuation-date 2026-10-30
