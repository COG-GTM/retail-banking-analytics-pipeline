import json
import os
import re
import secrets
import shutil
import subprocess
import time
import uuid
from pathlib import Path

import pytest


ROOT = Path(__file__).resolve().parents[1]
PIPELINE_NAME = "pl_retail_banking_daily"
SJD_TO_MODULE = {
    "sjd_customer_segments": "customer_segments.py",
    "sjd_txn_analytics": "txn_analytics.py",
    "sjd_risk_scoring": "risk_scoring.py",
    "sjd_master_profile": "master_profile.py",
}
PROC_NAMES = {
    "Load_stg_customer_360": "stg.usp_load_stg_customer_360",
    "Load_stg_txn_summary": "stg.usp_load_stg_txn_summary",
    "Load_stg_risk_factors": "stg.usp_load_stg_risk_factors",
}
PROC_FILES = {
    activity: f"synapse/sql/procs/{proc_name}.sql"
    for activity, proc_name in PROC_NAMES.items()
}
SQL_POOL = "sqlpool_retail"
SPARK_POOL = "sparkpool_retail"
LOAD_ACTIVITIES = [
    "Load_stg_customer_360",
    "Load_stg_txn_summary",
    "Load_stg_risk_factors",
]
SPARK_ACTIVITIES = {
    "Run_sjd_customer_segments": "sjd_customer_segments",
    "Run_sjd_txn_analytics": "sjd_txn_analytics",
    "Run_sjd_risk_scoring": "sjd_risk_scoring",
    "Run_sjd_master_profile": "sjd_master_profile",
}
EXPECTED_DEPS = {
    **{name: {} for name in LOAD_ACTIVITIES},
    **{
        name: {load: {"Succeeded"} for load in LOAD_ACTIVITIES}
        for name in (
            "Run_sjd_customer_segments",
            "Run_sjd_txn_analytics",
            "Run_sjd_risk_scoring",
        )
    },
    "Run_sjd_master_profile": {
        "Run_sjd_customer_segments": {"Succeeded"},
        "Run_sjd_txn_analytics": {"Succeeded"},
        "Run_sjd_risk_scoring": {"Succeeded"},
    },
    "Validate_post_run_row_counts": {
        "Run_sjd_master_profile": {"Succeeded"}
    },
    "Fail_pipeline": {
        "Validate_post_run_row_counts": {"Failed", "Skipped"}
    },
}
JSON_DIRECTORIES = (
    "synapse/pipelines",
    "synapse/sparkJobDefinitions",
    "synapse/triggers",
    "synapse/config",
)
EXPECTED_JSON_FILES = {
    "synapse/pipelines/pl_retail_banking_daily.json",
    "synapse/sparkJobDefinitions/sjd_customer_segments.json",
    "synapse/sparkJobDefinitions/sjd_txn_analytics.json",
    "synapse/sparkJobDefinitions/sjd_risk_scoring.json",
    "synapse/sparkJobDefinitions/sjd_master_profile.json",
    "synapse/triggers/tr_daily_0200.json",
    "synapse/config/pipeline_config.dev.json",
    "synapse/config/pipeline_config.uat.json",
    "synapse/config/pipeline_config.prod.json",
}
JSON_FILES = sorted(
    path
    for directory in JSON_DIRECTORIES
    for path in (ROOT / directory).glob("*.json")
)
RUN_DATE_EXPRESSION = (
    "@if(empty(pipeline().parameters.AsOfDate), "
    "formatDateTime(pipeline().TriggerTime, 'yyyy-MM-dd'), "
    "pipeline().parameters.AsOfDate)"
)
CONFIG_PATHS = {
    env: ROOT / f"synapse/config/pipeline_config.{env}.json"
    for env in ("dev", "uat", "prod")
}
EXPECTED_SCHEMAS = {
    "DB_CORE": "core",
    "DB_TXN": "txn",
    "DB_STG": "stg",
    "DB_DP": "dp",
}
DP_COUNTS = {
    "CUSTOMER_SEGMENTS": 407,
    "TRANSACTION_ANALYTICS": 500,
    "CUSTOMER_RISK_SCORES": 407,
    "CUSTOMER_MASTER_PROFILE": 407,
}
ACTIVITY_NAMES = list(EXPECTED_DEPS)
ACTIVITY_POLICY = {
    "timeout": "0.02:00:00",
    "retry": 0,
    "retryIntervalInSeconds": 30,
    "secureOutput": False,
    "secureInput": False,
}


def read_json(path):
    return json.loads(path.read_text(encoding="utf-8"))


def get_pipeline():
    return read_json(ROOT / "synapse/pipelines/pl_retail_banking_daily.json")


def get_activities():
    return get_pipeline()["properties"]["activities"]


@pytest.mark.parametrize("artifact_path", JSON_FILES, ids=lambda path: path.name)
def test_all_synapse_json_files_parse(artifact_path):
    assert isinstance(read_json(artifact_path), dict)


def test_synapse_json_file_set_is_exact():
    actual = {
        path.relative_to(ROOT).as_posix()
        for directory in JSON_DIRECTORIES
        for path in (ROOT / directory).glob("*.json")
    }
    assert actual == EXPECTED_JSON_FILES


@pytest.mark.parametrize(
    "artifact_path",
    [
        ROOT / "synapse/pipelines/pl_retail_banking_daily.json",
        *sorted((ROOT / "synapse/sparkJobDefinitions").glob("*.json")),
        ROOT / "synapse/triggers/tr_daily_0200.json",
    ],
    ids=lambda path: path.name,
)
def test_synapse_artifact_schema(artifact_path):
    artifact = read_json(artifact_path)
    assert artifact["name"] == artifact_path.stem
    assert isinstance(artifact["properties"], dict)

    if artifact_path.parent.name != "pipelines":
        return

    activities = artifact["properties"]["activities"]
    assert isinstance(activities, list)
    activity_names = [activity["name"] for activity in activities]
    assert len(activity_names) == len(set(activity_names))
    existing_names = set(activity_names)
    conditions_allowed = {"Succeeded", "Failed", "Skipped", "Completed"}

    for activity in activities:
        assert {"name", "type", "dependsOn", "typeProperties"} <= activity.keys()
        assert isinstance(activity["dependsOn"], list)
        assert isinstance(activity["typeProperties"], dict)
        assert isinstance(activity.get("description"), str)
        assert isinstance(activity.get("userProperties"), list)
        if activity["name"] == "Validate_post_run_row_counts":
            assert activity["userProperties"] == [
                {
                    "name": "source_file",
                    "value": "synapse/sql/validation/post_run_row_counts.sql",
                }
            ]
        else:
            assert activity["userProperties"] == []
        if activity["type"] == "Fail":
            assert "policy" not in activity
        else:
            assert activity["policy"] == ACTIVITY_POLICY
        for dependency in activity["dependsOn"]:
            assert dependency["activity"] in existing_names
            dependency_conditions = dependency["dependencyConditions"]
            assert dependency_conditions
            assert set(dependency_conditions) <= conditions_allowed

    dependencies = {name: 0 for name in activity_names}
    downstream = {name: [] for name in activity_names}
    for activity in activities:
        for dependency in activity["dependsOn"]:
            upstream = dependency["activity"]
            dependencies[activity["name"]] += 1
            downstream[upstream].append(activity["name"])

    ready = [name for name, count in dependencies.items() if count == 0]
    visited = 0
    while ready:
        current = ready.pop()
        visited += 1
        for child in downstream[current]:
            dependencies[child] -= 1
            if dependencies[child] == 0:
                ready.append(child)
    assert visited == len(activity_names), "pipeline activity graph contains a cycle"


def test_pipeline_dag_matches_contract():
    actual = {
        activity["name"]: {
            dependency["activity"]: set(dependency["dependencyConditions"])
            for dependency in activity["dependsOn"]
        }
        for activity in get_activities()
    }
    assert actual == EXPECTED_DEPS


def evaluate_pipeline(forced_failure=None):
    statuses = {}
    pending = {activity["name"]: activity for activity in get_activities()}
    while pending:
        progressed = False
        for name, activity in list(pending.items()):
            dependencies = activity["dependsOn"]
            if not all(dependency["activity"] in statuses for dependency in dependencies):
                continue
            can_run = all(
                statuses[dependency["activity"]]
                in dependency["dependencyConditions"]
                for dependency in dependencies
            )
            if not can_run:
                status = "Skipped"
            elif name == forced_failure:
                status = "Failed"
            elif activity["type"] == "Fail":
                status = "Failed"
            else:
                status = "Succeeded"
            statuses[name] = status
            del pending[name]
            progressed = True
        assert progressed, "pipeline dependency graph could not be evaluated"
    return statuses


@pytest.mark.parametrize(
    "activity_name",
    [name for name in ACTIVITY_NAMES if name != "Fail_pipeline"],
)
def test_any_non_fail_activity_failure_runs_fail_pipeline(activity_name):
    statuses = evaluate_pipeline(forced_failure=activity_name)
    assert statuses[activity_name] == "Failed"
    assert statuses["Fail_pipeline"] == "Failed"


def test_fail_pipeline_is_skipped_when_all_activities_succeed():
    statuses = evaluate_pipeline()
    assert statuses["Validate_post_run_row_counts"] == "Succeeded"
    assert statuses["Fail_pipeline"] == "Skipped"


def test_stored_procedure_activities_match_contract():
    pipeline = get_pipeline()
    config = read_json(CONFIG_PATHS["dev"])
    activities = {activity["name"]: activity for activity in get_activities()}
    for activity_name, expected_proc in PROC_NAMES.items():
        activity = activities[activity_name]
        assert activity["type"] == "SqlPoolStoredProcedure"
        properties = activity["typeProperties"]
        assert activity["sqlPool"]["referenceName"] == SQL_POOL
        assert activity["sqlPool"]["type"] == "SqlPoolReference"
        actual_proc = properties["storedProcedureName"].replace("[", "").replace("]", "")
        assert actual_proc == expected_proc
        assert f"synapse/sql/procs/{actual_proc}.sql" == PROC_FILES[activity_name]
        as_of_date = properties["storedProcedureParameters"]["AsOfDate"]
        expression = as_of_date["value"]
        assert as_of_date["type"] == "String"
        assert expression["type"] == "Expression"
        assert expression["value"] == RUN_DATE_EXPRESSION

    lookback = activities["Load_stg_txn_summary"]["typeProperties"][
        "storedProcedureParameters"
    ]["LookbackMonths"]
    assert lookback == {
        "value": config["run_params"]["LOOKBACK_MONTHS"],
        "type": "Int32",
    }
    assert pipeline["properties"]["parameters"]["AsOfDate"]["defaultValue"] == ""


def test_spark_activities_match_contract_and_environment_config():
    activities = {activity["name"]: activity for activity in get_activities()}
    configs = {env: read_json(path) for env, path in CONFIG_PATHS.items()}
    for activity_name, sjd_name in SPARK_ACTIVITIES.items():
        activity = activities[activity_name]
        assert activity["type"] == "SparkJob"
        properties = activity["typeProperties"]
        assert properties["sparkJob"]["referenceName"] == sjd_name
        assert properties["targetBigDataPool"]["referenceName"] == SPARK_POOL
        args = properties["args"]
        as_of_date_index = args.index("--as-of-date")
        assert args[as_of_date_index + 1] == {
            "value": RUN_DATE_EXPRESSION,
            "type": "Expression",
        }
        env_index = args.index("--env")
        assert args[env_index + 1] == {
            "value": "@pipeline().parameters.Env",
            "type": "Expression",
        }
        lookback_index = args.index("--lookback-months")
        threshold_index = args.index("--risk-score-threshold")
        for config in configs.values():
            assert args[lookback_index + 1] == str(
                config["run_params"]["LOOKBACK_MONTHS"]
            )
            assert args[threshold_index + 1] == str(
                config["run_params"]["RISK_SCORE_THRESHOLD"]
            )


def test_spark_job_definitions_match_modules_and_dev_storage():
    dev_code_root = read_json(CONFIG_PATHS["dev"])["adls"]["code_root"]
    definition_paths = sorted((ROOT / "synapse/sparkJobDefinitions").glob("*.json"))
    assert len(definition_paths) == 4
    for path in definition_paths:
        artifact = read_json(path)
        name = artifact["name"]
        module = SJD_TO_MODULE[name]
        properties = artifact["properties"]
        job_properties = properties["jobProperties"]
        assert properties["targetBigDataPool"]["referenceName"] == SPARK_POOL
        assert properties["language"] == "python"
        assert properties["requiredSparkVersion"] == "3.4"
        assert job_properties["file"].endswith(f"synapse/spark/jobs/{module}")
        assert job_properties["file"].startswith(dev_code_root)
        assert any(
            file_name.endswith("synapse.zip")
            for file_name in job_properties["pyFiles"]
        )


def test_daily_trigger_matches_schedule_and_legacy_cron():
    trigger = read_json(ROOT / "synapse/triggers/tr_daily_0200.json")
    properties = trigger["properties"]
    assert properties["type"] == "ScheduleTrigger"
    recurrence = properties["typeProperties"]["recurrence"]
    assert recurrence["frequency"] == "Day"
    assert recurrence["interval"] == 1
    assert recurrence["schedule"]["hours"] == [2]
    assert recurrence["schedule"]["minutes"] == [0]
    assert recurrence["timeZone"] == "UTC"
    pipeline = properties["pipelines"][0]
    assert pipeline["pipelineReference"]["referenceName"] == PIPELINE_NAME
    assert "trigger().scheduledTime" in pipeline["parameters"]["AsOfDate"]
    assert "Env" in pipeline["parameters"]

    in_code_block = False
    cron_fields = None
    for line in (ROOT / "docs/pipeline_flow.md").read_text(encoding="utf-8").splitlines():
        if line.strip().startswith("```"):
            in_code_block = not in_code_block
        elif in_code_block and "run_full_pipeline.sh" in line:
            cron_fields = line.split()
            break
    assert cron_fields is not None
    assert int(cron_fields[0]) == recurrence["schedule"]["minutes"][0]
    assert int(cron_fields[1]) == recurrence["schedule"]["hours"][0]


@pytest.mark.parametrize("env", ["dev", "uat", "prod"])
def test_environment_configs_are_named_secret_free_and_consistent(env):
    path = CONFIG_PATHS[env]
    config = read_json(path)
    assert config["env"] == env
    assert config["schemas"] == EXPECTED_SCHEMAS
    assert config["run_params"]["LOOKBACK_MONTHS"] == 12
    assert config["run_params"]["RISK_SCORE_THRESHOLD"] == 700
    assert config["sql_pool"]["name"] == SQL_POOL
    assert config["spark_pool"]["name"] == SPARK_POOL
    assert config["key_vault"]["name"]

    raw = path.read_text(encoding="utf-8").casefold()
    for forbidden in (
        "password",
        "pwd=",
        "accountkey=",
        "sharedaccesssignature",
        "sig=",
        "client_secret",
    ):
        assert forbidden not in raw


def test_validation_sql_is_inlined_in_script_activity():
    sql_path = ROOT / "synapse/sql/validation/post_run_row_counts.sql"
    sql_text = sql_path.read_text(encoding="utf-8")
    for table in DP_COUNTS:
        assert f"dp.{table}" in sql_text
    assert "UNION ALL" in sql_text
    assert "THROW" in sql_text

    validation = {
        activity["name"]: activity for activity in get_activities()
    }["Validate_post_run_row_counts"]
    script = validation["typeProperties"]["scripts"][0]
    assert script["type"] == "Query"
    assert script["text"] == sql_text
    assert validation["userProperties"] == [
        {
            "name": "source_file",
            "value": "synapse/sql/validation/post_run_row_counts.sql",
        }
    ]
    assert validation["linkedServiceName"] == {
        "referenceName": SQL_POOL,
        "type": "LinkedServiceReference",
    }


def test_migration_docs_and_readme_cover_the_migration():
    docs = (ROOT / "docs/synapse_migration.md").read_text(encoding="utf-8")
    for unit_number in range(1, 11):
        assert f"AZSYN-{unit_number:02d}" in docs
    for activity_name in ACTIVITY_NAMES:
        assert activity_name in docs

    readme_lines = (ROOT / "README.md").read_text(encoding="utf-8").splitlines()
    assert readme_lines[0] == "# Retail Banking Customer Analytics Pipeline"
    assert "## Azure Synapse migration" in readme_lines


def test_no_python_cache_files_are_tracked():
    if shutil.which("git") is None:
        pytest.skip("git is unavailable")
    result = subprocess.run(
        ["git", "ls-files"],
        cwd=ROOT,
        capture_output=True,
        check=True,
        text=True,
    )
    tracked = result.stdout.splitlines()
    assert not [
        path
        for path in tracked
        if "__pycache__" in Path(path).parts or path.endswith(".pyc")
    ]


def _sqlcmd(container_name, password, query):
    return subprocess.run(
        [
            "docker",
            "exec",
            "-e",
            f"SQLCMDPASSWORD={password}",
            container_name,
            "/opt/mssql-tools18/bin/sqlcmd",
            "-C",
            "-b",
            "-W",
            "-S",
            "localhost",
            "-U",
            "sa",
            "-P",
            password,
            "-Q",
            query,
        ],
        capture_output=True,
        text=True,
        timeout=60,
        env={**os.environ, "SQLCMDPASSWORD": password},
    )


@pytest.fixture(scope="module")
def docker_sql_server():
    docker = shutil.which("docker")
    if docker is None:
        pytest.skip("Docker is not installed")
    try:
        docker_info = subprocess.run(
            [docker, "info"],
            capture_output=True,
            text=True,
            timeout=20,
        )
    except subprocess.TimeoutExpired:
        pytest.skip("Docker daemon did not respond")
    if docker_info.returncode != 0:
        reason = docker_info.stderr.strip() or docker_info.stdout.strip()
        pytest.skip(f"Docker daemon is unavailable: {reason}")

    image = "mcr.microsoft.com/mssql/server:2022-latest"
    try:
        pull = subprocess.run(
            [docker, "pull", image],
            capture_output=True,
            text=True,
            timeout=600,
        )
    except subprocess.TimeoutExpired:
        pytest.skip("SQL Server image pull timed out")
    if pull.returncode != 0:
        reason = pull.stderr.strip() or pull.stdout.strip()
        pytest.skip(f"Could not pull SQL Server image: {reason}")

    container_name = f"synapse-validation-{uuid.uuid4().hex[:12]}"
    password = f"Aa1!{secrets.token_hex(20)}"
    start = subprocess.run(
        [
            docker,
            "run",
            "-d",
            "--name",
            container_name,
            "-e",
            "ACCEPT_EULA=Y",
            "-e",
            f"MSSQL_SA_PASSWORD={password}",
            image,
        ],
        capture_output=True,
        text=True,
        timeout=120,
    )
    if start.returncode != 0:
        pytest.fail(f"Could not start SQL Server container: {start.stderr.strip()}")

    try:
        last_error = ""
        for _ in range(90):
            ready = _sqlcmd(container_name, password, "SELECT 1")
            if ready.returncode == 0:
                break
            last_error = ready.stderr.strip() or ready.stdout.strip()
            time.sleep(2)
        else:
            pytest.fail(f"SQL Server did not become ready: {last_error}")
        yield container_name, password
    finally:
        subprocess.run(
            [docker, "rm", "-f", container_name],
            capture_output=True,
            text=True,
            timeout=60,
        )


@pytest.mark.skipif(shutil.which("docker") is None, reason="Docker is not installed")
def test_validation_sql_on_sql_server(docker_sql_server):
    container_name, password = docker_sql_server
    schema_result = _sqlcmd(container_name, password, "CREATE SCHEMA dp;")
    assert schema_result.returncode == 0, schema_result.stdout + schema_result.stderr

    for table in DP_COUNTS:
        create_result = _sqlcmd(
            container_name,
            password,
            f"CREATE TABLE dp.{table} (id INT NOT NULL);",
        )
        assert create_result.returncode == 0, (
            create_result.stdout + create_result.stderr
        )
        count = DP_COUNTS[table]
        seed_result = _sqlcmd(
            container_name,
            password,
            f"""
WITH numbers AS (
    SELECT TOP ({count}) ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS id
    FROM sys.all_objects AS a CROSS JOIN sys.all_objects AS b
)
INSERT INTO dp.{table} (id)
SELECT id FROM numbers;
""",
        )
        assert seed_result.returncode == 0, seed_result.stdout + seed_result.stderr

    sql_text = (ROOT / "synapse/sql/validation/post_run_row_counts.sql").read_text(
        encoding="utf-8"
    )
    sql_server_compatible = re.sub(
        r"\s+WITH\s*\(\s*DISTRIBUTION\s*=\s*ROUND_ROBIN\s*,\s*HEAP\s*\)",
        "",
        sql_text,
        flags=re.IGNORECASE,
    )
    validation_result = _sqlcmd(
        container_name,
        password,
        sql_server_compatible,
    )
    assert validation_result.returncode == 0, (
        validation_result.stdout + validation_result.stderr
    )
    for table, count in DP_COUNTS.items():
        assert re.search(
            rf"{re.escape(table)}\s+{count}\b",
            validation_result.stdout,
        ), validation_result.stdout

    delete_result = _sqlcmd(
        container_name,
        password,
        "DELETE FROM dp.CUSTOMER_RISK_SCORES;",
    )
    assert delete_result.returncode == 0, delete_result.stdout + delete_result.stderr
    failure_result = _sqlcmd(
        container_name,
        password,
        sql_server_compatible,
    )
    failure_output = failure_result.stdout + failure_result.stderr
    assert failure_result.returncode != 0
    assert "50098" in failure_output
    assert "CUSTOMER_RISK_SCORES" in failure_output
