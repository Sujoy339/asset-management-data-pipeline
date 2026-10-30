# Sample Execution

## Successful execution

Command:
python src/pipeline.py

Expected:
- Bronze ingestion completed
- Silver validation completed
- Gold outputs generated
- Reconciliation generated

## Failure execution

Command:
python src/pipeline.py --fail-after-silver

Expected:
- Bronze completed
- Silver completed
- Controlled failure
- Gold not published by failed run

## Recovery

Command:
python src/pipeline.py

Expected:
- Pipeline restarts
- Gold outputs are generated
- Reconciliation is available