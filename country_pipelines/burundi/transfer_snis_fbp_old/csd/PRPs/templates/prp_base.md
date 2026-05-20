name: "Generic DHIS2 Pipeline PRP Template"
description: |

## Purpose
This PRP provides a structured and context-rich prompt template for creating pipelines that interact with one or more DHIS2 instances using OpenHEXA conventions.

It ensures any AI agent has:
- ✅ Full context (docs, examples, patterns)
- ✅ Guidance on structure and gotchas
- ✅ A format for testable and composable code

---

## 🧭 Goal

Build a DHIS2-compatible OpenHEXA pipeline that achieves a defined objective such as:
- Reading data from a DHIS2 dataset or API endpoint
- Transforming or aggregating it (optional)
- Pushing data back into the same or another DHIS2 instance

The pipeline:
- Uses only `@pipeline`, `@task`, and `@parameter` decorators
- Accepts parameters via the OpenHEXA UI or CLI
- Should rely on [`openhexa-toolbox`](https://github.com/BLSQ/openhexa-toolbox/tree/main/openhexa/toolbox/dhis2) for DHIS2 operations

---

## Why

- Enables repeatable and reusable DHIS2 integrations in a declarative way
- Reduces the complexity of manually handling API requests, tokens, and payload structures
- Adheres to OpenHEXA's pipeline standards and automation patterns
- Ensures proper use of dataset IDs, orgUnits, dataElement IDs, periods, and mappings

---

## What

### Expected Features
- [ ] Accept one or more DHIS2 connections as input (`ToolboxDHIS2Client`)
- [ ] Handle a specific dataset or custom query
- [ ] Apply any necessary transformations (e.g., mapping, validation)
- [ ] Respect the DHIS2 dataValueSet format for both input and output
- [ ] Be testable and traceable via logs

---

## ✅ Success Criteria

- [ ] Uses `@pipeline`, `@task`, and `@parameter` decorators only
- [ ] Includes at least one end-to-end example in the README
- [ ] Handles periods in `YYYYQn` or `YYYYMM` format
- [ ] Reads from `connections` in `workspace.yaml`
- [ ] Pushes only mapped and valid payloads

---

## 📚 All Needed Context

### Key Docs
```yaml
- url: https://docs.dhis2.org/fr/develop/using-the-api/dhis-core-version-242/introduction.html
  why: Reference for endpoints such as /dataValueSets, /dataElements, etc.

- url: https://github.com/BLSQ/openhexa-toolbox/tree/main/openhexa/toolbox/dhis2
  why: Always use the DHIS2 helper client from here to abstract low-level logic

- url: https://github.com/BLSQ/openhexa/wiki/Writing-OpenHEXA-pipelines
  why: Follow parameter and task patterns defined here
```

## Directory structure:
pipelines/dhis2_pipeline_name/
├── pipeline.py          # Contains all tasks and parameters
├── README.md            # Explains parameters, behavior, expected input/output
└── requirements.txt     # Must include openhexa-toolbox, requests, etc.


## Blueprints
### sample parameters
@parameter("source_connection", type=str, default="dhis2-source", description="DHIS2 connection for source instance")
@parameter("target_connection", type=str, default="dhis2-target", description="DHIS2 connection for destination instance")
@parameter("period", type=str, description="Period to extract, e.g. 2024Q1")
@parameter("dataset_id", type=str, description="DHIS2 dataset ID to pull from")
@parameter("mapping_file", type=File, description="JSON file containing mappings for dataElements and COCs")

## Gotcha and  best practices
 Gotchas & Best Practices

## Tasks:
Task 1:
  READ from source DHIS2 using dataset_id + period

Task 2:
  LOAD mapping_file (JSON with dataElement + COC mappings)

Task 3:
  TRANSFORM dataset values according to mapping

Task 4:
  PUSH values to target DHIS2 instance

Task 5:
  LOG number of values pushed, skipped, and mapped

Task 6:
  VALIDATE structure of final payload (5 required fields)

## Validation
### Linting
ruff check pipelines/dhis2_pipeline_name/pipeline.py --fix
 
mypy pipelines/dhis2_pipeline_name/pipeline.py
### Sanity Tests
def test_value_mapping():
    assert mapping['dataElements']['abc123'] == 'xyz789'
### CLI Dry Run

openhexa pipeline run --name my-dhis2-pipeline --params period=2024Q1 dataset_id=xyz mapping_file=map.json

### Anti-Patterns
❌ Don’t use low-level requests when openhexa-toolbox has it

❌ Don’t skip parameter validations

❌ Don’t push duplicate dataElements with no value

❌ Don’t hardcode orgUnit, periods, or dataset_id