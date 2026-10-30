
#!/bin/bash
set -e

for SCALE in 1 10 100
do
    echo "Running scale ${SCALE}"

    python src/generate_data.py --scale "$SCALE"

    /usr/bin/time -p python src/pipeline.py
done
