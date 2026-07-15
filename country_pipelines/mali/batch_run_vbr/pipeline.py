"""Orchestrator pipeline that launches many runs of a target pipeline in batches.

Given a target pipeline code and a CSV of parameter combinations (one run per row),
this pipeline launches the runs in batches of a fixed size, waiting for every run in a
batch to reach a terminal state before starting the next batch. This bounds the load on
the OpenHEXA backend while parallelizing a parameter sweep (typically over ``run_vbr``).
"""

import csv
import os
import time
from concurrent.futures import ThreadPoolExecutor

from openhexa.sdk import current_run, parameter, pipeline, workspace
from openhexa.toolbox.hexa import OpenHEXA

import config as batch_config


@pipeline("batch_run_vbr")
@parameter(
    "target_pipeline",
    name="Pipeline to run",
    help="Code of the pipeline whose runs will be launched in batches (e.g. run-vbr)",
    type=str,
    default="run-vbr",
    required=True,
)
@parameter(
    "workspace_slug",
    name="Workspace slug",
    help="Slug of the workspace the target pipeline lives in",
    type=str,
    default="vbr-mali",
    required=True,
)
@parameter(
    "configs_csv",
    name="Configurations CSV",
    help=(
        "CSV file name (under pipelines/batch_run_vbr/) with one row per run; "
        "columns are the target pipeline's parameter codes"
    ),
    type=str,
    required=True,
    default="batch_configs.csv",
)
def batch_run_vbr(
    target_pipeline: str,
    workspace_slug: str,
    configs_csv: str,
):
    """Launch a target pipeline over a CSV of parameter combinations, in batches.

    The batch size, OpenHEXA connection identifier and poll interval are read from
    ``config.py`` (``DEFAULT_BATCH_SIZE``, ``DEFAULT_OPENHEXA_CONN``,
    ``DEFAULT_POLL_INTERVAL``), not exposed as parameters.

    Parameters
    ----------
    target_pipeline : str
        Code of the pipeline to launch (e.g. ``run-vbr``).
    workspace_slug : str
        Slug of the workspace the target pipeline lives in.
    configs_csv : str
        Name of the CSV file (under ``pipelines/batch_run_vbr/``) listing one run per row.
    """
    if target_pipeline in ("batch_run_vbr", "batch-run-vbr"):
        raise ValueError("target_pipeline cannot be the batch runner itself (would recurse).")
    if batch_config.DEFAULT_BATCH_SIZE < 1:
        raise ValueError("DEFAULT_BATCH_SIZE must be at least 1.")

    hexa = get_hexa_connection(batch_config.DEFAULT_OPENHEXA_CONN)
    configs = load_configs(configs_csv)
    pipeline_id, version_id = resolve_pipeline(hexa, workspace_slug, target_pipeline)

    run_batches(
        hexa,
        workspace_slug,
        target_pipeline,
        pipeline_id,
        version_id,
        configs,
        batch_config.DEFAULT_BATCH_SIZE,
        batch_config.DEFAULT_POLL_INTERVAL,
    )


def get_hexa_connection(openhexa_conn: str) -> OpenHEXA:
    """Build an authenticated OpenHEXA client from a workspace custom connection.

    Parameters
    ----------
    openhexa_conn : str
        Identifier of the custom connection declaring ``mail`` and ``password`` fields.

    Returns
    -------
    OpenHEXA
        An authenticated OpenHEXA client.
    """
    con = workspace.custom_connection(openhexa_conn)
    hexa = OpenHEXA(batch_config.SERVER_URL, username=con.mail, password=con.password)
    current_run.log_info("Connected to OpenHEXA")
    return hexa


def load_configs(configs_csv: str) -> list[dict]:
    """Read the parameter-combinations CSV into a list of run config dicts.

    Each row becomes one config dict. Cells are coerced to the type expected by the target
    pipeline (see ``PARAM_TYPES``); blank cells are omitted so the target pipeline falls
    back to its own defaults.

    Parameters
    ----------
    configs_csv : str
        Name of the CSV file under ``pipelines/batch_run_vbr/``.

    Returns
    -------
    list of dict
        One config dict per data row.
    """
    csv_path = os.path.join(
        workspace.files_path, "pipelines", "batch_run_vbr", configs_csv
    )
    if not os.path.exists(csv_path):
        raise FileNotFoundError(f"Configurations CSV not found: {csv_path}")

    configs: list[dict] = []
    with open(csv_path, newline="", encoding="utf-8") as file:
        reader = csv.DictReader(file)
        unknown = [c for c in (reader.fieldnames or []) if c not in batch_config.PARAM_TYPES]
        if unknown:
            current_run.log_warning(
                f"CSV has columns not known to the target pipeline (passed as strings): {unknown}"
            )
        for row_number, row in enumerate(reader, start=1):
            config = _coerce_row(row)
            if not config:
                current_run.log_warning(f"Row {row_number} is empty, skipping.")
                continue
            configs.append(config)

    if not configs:
        raise ValueError(f"No run configurations found in {csv_path}.")

    current_run.log_info(f"Loaded {len(configs)} run configuration(s) from {configs_csv}")
    return configs


def _coerce_row(row: dict) -> dict:
    """Coerce one CSV row to a run config, dropping blank cells.

    Parameters
    ----------
    row : dict
        Raw string-valued row from ``csv.DictReader``.

    Returns
    -------
    dict
        Config dict with values cast via ``PARAM_TYPES``; empty cells omitted.
    """
    config: dict = {}
    for key, raw in row.items():
        if key is None or raw is None:
            continue
        value = raw.strip()
        if not value:
            continue
        caster = batch_config.PARAM_TYPES.get(key, str)
        config[key] = caster(value)
    return config


def resolve_pipeline(hexa: OpenHEXA, workspace_slug: str, target_pipeline: str) -> tuple[str, str]:
    """Resolve a pipeline code to its id and current version id.

    Parameters
    ----------
    hexa : OpenHEXA
        Authenticated OpenHEXA client.
    workspace_slug : str
        Slug of the workspace the pipeline lives in.
    target_pipeline : str
        Code of the pipeline to resolve.

    Returns
    -------
    tuple of (str, str)
        The pipeline id and its current version id.
    """
    pipeline_info = hexa.get_pipeline(workspace_slug, target_pipeline)["pipelineByCode"]
    if pipeline_info is None:
        raise ValueError(
            f"Pipeline '{target_pipeline}' not found in workspace '{workspace_slug}'."
        )
    pipeline_id = pipeline_info["id"]
    version_id = pipeline_info["currentVersion"]["id"]
    current_run.log_info(
        f"Resolved pipeline '{target_pipeline}' -> id={pipeline_id}, version={version_id}"
    )
    return pipeline_id, version_id


def _launch_and_wait(
    hexa: OpenHEXA,
    pipeline_id: str,
    version_id: str,
    config: dict,
    poll_interval: int,
    index: int,
) -> dict:
    """Launch one run and poll it until it reaches a terminal state.

    Parameters
    ----------
    hexa : OpenHEXA
        Authenticated OpenHEXA client.
    pipeline_id : str
        Id of the pipeline to run.
    version_id : str
        Version id to run.
    config : dict
        Parameter configuration for this run.
    poll_interval : int
        Seconds between status checks.
    index : int
        1-based index of the config, used only for logging.

    Returns
    -------
    dict
        A summary: ``index``, ``run_id``, ``status``, ``duration``.
    """
    run = hexa.run_pipeline(
        id=pipeline_id,
        config=config,
        version_id=version_id,
        send_notification=batch_config.SEND_NOTIFICATION,
    )
    run_id = run["id"]
    current_run.log_info(f"[run {index}] launched run {run_id} with config {config}")

    last_status = None
    while True:
        details = hexa.get_pipeline_run(run_id)
        status = details.get("status")
        if status != last_status:
            current_run.log_info(f"[run {index}] {run_id} -> {status}")
            last_status = status
        if status in batch_config.TERMINAL_STATUSES:
            return {
                "index": index,
                "run_id": run_id,
                "status": status,
                "duration": details.get("duration"),
            }
        time.sleep(poll_interval)


def run_batches(
    hexa: OpenHEXA,
    workspace_slug: str,
    target_pipeline: str,
    pipeline_id: str,
    version_id: str,
    configs: list[dict],
    batch_size: int,
    poll_interval: int,
):
    """Launch runs in batches, waiting for each batch to finish before the next.

    Parameters
    ----------
    hexa : OpenHEXA
        Authenticated OpenHEXA client.
    workspace_slug : str
        Slug of the workspace the target pipeline lives in (used for run URLs).
    target_pipeline : str
        Code of the pipeline being launched (used for run URLs).
    pipeline_id : str
        Id of the pipeline to run.
    version_id : str
        Version id to run.
    configs : list of dict
        Run configurations, one per launch.
    batch_size : int
        Number of runs launched concurrently per batch.
    poll_interval : int
        Seconds between status checks.
    """
    results: list[dict] = []
    total_batches = (len(configs) + batch_size - 1) // batch_size

    for batch_number, start in enumerate(range(0, len(configs), batch_size), start=1):
        batch = configs[start : start + batch_size]
        current_run.log_info(
            f"Starting batch {batch_number}/{total_batches} with {len(batch)} run(s)"
        )

        with ThreadPoolExecutor(max_workers=batch_size) as executor:
            futures = [
                executor.submit(
                    _launch_and_wait,
                    hexa,
                    pipeline_id,
                    version_id,
                    config,
                    poll_interval,
                    start + offset + 1,
                )
                for offset, config in enumerate(batch)
            ]
            batch_results = [future.result() for future in futures]

        results.extend(batch_results)
        current_run.log_info(f"Batch {batch_number}/{total_batches} finished")

    _report(results, workspace_slug, target_pipeline)


def _report(results: list[dict], workspace_slug: str, target_pipeline: str):
    """Log a final summary of every run and flag any that did not succeed.

    Parameters
    ----------
    results : list of dict
        Per-run summaries produced by ``_launch_and_wait``.
    workspace_slug : str
        Slug of the workspace, used to build run URLs.
    target_pipeline : str
        Code of the launched pipeline, used to build run URLs.
    """
    results = sorted(results, key=lambda item: item["index"])
    current_run.log_info("=== Batch run summary ===")
    for item in results:
        url = (
            f"{batch_config.SERVER_URL}/workspaces/{workspace_slug}"
            f"/pipelines/{target_pipeline}/runs/{item['run_id']}"
        )
        current_run.log_info(
            f"run {item['index']}: {item['status']} "
            f"(duration={item['duration']}) {url}"
        )

    failed = [item for item in results if item["status"] != "success"]
    if failed:
        current_run.log_error(
            f"{len(failed)}/{len(results)} run(s) did not succeed: "
            f"{[item['run_id'] for item in failed]}"
        )
    else:
        current_run.log_info(f"All {len(results)} run(s) succeeded.")


if __name__ == "__main__":
    batch_run_vbr()
