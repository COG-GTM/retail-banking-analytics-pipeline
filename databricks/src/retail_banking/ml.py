from collections.abc import Iterator
from contextlib import contextmanager

import mlflow


@contextmanager
def mlflow_run(cfg, run_name: str) -> Iterator[object]:
    if not cfg.mlflow_enabled:
        yield None
        return
    mlflow.set_experiment(cfg.mlflow_experiment)
    with mlflow.start_run(run_name=run_name) as run:
        yield run


def log_sklearn_model(model, name: str, params: dict, metrics: dict) -> None:
    if mlflow.active_run() is None:
        return
    mlflow.log_params(params)
    mlflow.log_metrics({key: float(value) for key, value in metrics.items()})
    try:
        from mlflow import sklearn as mlflow_sklearn

        mlflow_sklearn.log_model(model, name)
    except ImportError:
        pass
