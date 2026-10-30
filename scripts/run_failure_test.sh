
#!/bin/bash

set +e

python src/pipeline.py --fail-after-silver

FAILURE_CODE=$?

set -e

if [ "$FAILURE_CODE" -eq 0 ]; then
    echo "ERROR: Failure injection did not fail"
    exit 1
fi

echo "Controlled failure confirmed"

python src/pipeline.py

echo "Recovery execution completed"
