from __future__ import annotations

import hashlib
import io
import json
import os
import tempfile
import time

import joblib
import numpy as np
import ray
import torch
import xgboost
from pyarrow.fs import S3FileSystem
from ray import train
from ray.train import Checkpoint, RunConfig, ScalingConfig
from ray.train.torch import TorchTrainer, prepare_model
from ray.train.xgboost import RayTrainReportCallback, XGBoostTrainer
from sklearn.linear_model import LogisticRegression, SGDClassifier

ROWS = 256
RANDOM_SEED = 42
ROUNDS_PER_ATTEMPT = 4


def records() -> list[dict[str, float | int]]:
    return [
        {
            "x1": float(index),
            "x2": float(index % 7),
            "label": int((index + index // 7) % 2),
        }
        for index in range(ROWS)
    ]


def distributed_xgboost_worker(config: dict[str, object]) -> None:
    shard = train.get_dataset_shard("train")
    frame = shard.materialize().to_pandas()
    matrix = xgboost.DMatrix(frame.drop(columns=["label"]), label=frame["label"])
    delay = float(os.getenv("SCEPTRE_TRAIN_START_DELAY_SECONDS", "0"))
    if delay > 0:
        time.sleep(delay)
    checkpoint = train.get_checkpoint()
    prior_model = RayTrainReportCallback.get_model(checkpoint) if checkpoint else None
    xgboost.train(
        config,
        dtrain=matrix,
        num_boost_round=ROUNDS_PER_ATTEMPT,
        evals=[(matrix, "train")],
        callbacks=[RayTrainReportCallback()],
        xgb_model=prior_model,
    )


def distributed_torch_worker(config: dict[str, object]) -> None:
    torch.manual_seed(int(config["seed"]))
    model = prepare_model(torch.nn.Linear(2, 2))
    checkpoint = train.get_checkpoint()
    if checkpoint is not None:
        with checkpoint.as_directory() as checkpoint_dir:
            model.module.load_state_dict(
                torch.load(
                    f"{checkpoint_dir}/model.pt",
                    map_location="cpu",
                    weights_only=True,
                )
            )
    optimizer = torch.optim.SGD(model.parameters(), lr=0.01)
    features = torch.tensor([[float(index), float(index % 7)] for index in range(32)])
    labels = torch.tensor([(index + index // 7) % 2 for index in range(32)])
    loss_value = 0.0
    for _ in range(2):
        optimizer.zero_grad()
        loss = torch.nn.functional.cross_entropy(model(features), labels)
        loss.backward()
        optimizer.step()
        loss_value = float(loss.detach())
    with tempfile.TemporaryDirectory() as checkpoint_dir:
        torch.save(model.module.state_dict(), f"{checkpoint_dir}/model.pt")
        train.report(
            {"prior_checkpoint_restored": checkpoint is not None, "train_loss": loss_value},
            checkpoint=Checkpoint.from_directory(checkpoint_dir),
        )


def main() -> None:
    rows = records()
    features = np.asarray([[row["x1"], row["x2"]] for row in rows], dtype=float)
    target = np.asarray([row["label"] for row in rows], dtype=np.int64)

    ordinary = LogisticRegression(random_state=RANDOM_SEED, max_iter=500).fit(
        features,
        target,
    )
    ordinary_accuracy = float(np.mean(ordinary.predict(features) == target))

    incremental = SGDClassifier(loss="log_loss", random_state=RANDOM_SEED)
    classes = np.asarray([0, 1], dtype=np.int64)
    for offset in range(0, ROWS // 2, 32):
        incremental.partial_fit(
            features[offset : offset + 32],
            target[offset : offset + 32],
            classes=classes,
        )
    checkpoint = io.BytesIO()
    joblib.dump(incremental, checkpoint)
    checkpoint_digest = hashlib.sha256(checkpoint.getvalue()).hexdigest()
    checkpoint.seek(0)
    restored = joblib.load(checkpoint)
    for offset in range(ROWS // 2, ROWS, 32):
        restored.partial_fit(features[offset : offset + 32], target[offset : offset + 32])
    incremental_accuracy = float(np.mean(restored.predict(features) == target))

    storage_path = os.environ["SCEPTRE_TRAIN_STORAGE_PATH"].strip("/")
    storage_filesystem = S3FileSystem(
        access_key=os.environ["AWS_ACCESS_KEY_ID"],
        secret_key=os.environ["AWS_SECRET_ACCESS_KEY"],
        endpoint_override=os.environ["SCEPTRE_TRAIN_S3_ENDPOINT"],
        scheme="http",
    )
    run_name = os.getenv("SCEPTRE_TRAIN_RUN_NAME", "phase-0a-estimator-matrix")
    expected_prior_rounds = int(os.getenv("SCEPTRE_EXPECT_PRIOR_ROUNDS", "0"))
    trainer = XGBoostTrainer(
        distributed_xgboost_worker,
        train_loop_config={
            "objective": "binary:logistic",
            "eval_metric": "logloss",
            "seed": RANDOM_SEED,
            "nthread": 1,
        },
        datasets={"train": ray.data.from_items(rows, override_num_blocks=4)},
        scaling_config=ScalingConfig(
            num_workers=2,
            use_gpu=False,
            resources_per_worker={"CPU": 1},
        ),
        run_config=RunConfig(
            name=f"{run_name}-xgboost",
            storage_path=storage_path,
            storage_filesystem=storage_filesystem,
        ),
    )
    result = trainer.fit()
    if result.checkpoint is None:
        raise RuntimeError("Ray Train did not produce an XGBoost checkpoint.")
    booster = RayTrainReportCallback.get_model(result.checkpoint)
    expected_rounds = expected_prior_rounds + ROUNDS_PER_ATTEMPT
    if booster.num_boosted_rounds() != expected_rounds:
        raise RuntimeError("Restored XGBoost checkpoint has the wrong round count.")

    torch_result = TorchTrainer(
        distributed_torch_worker,
        train_loop_config={"seed": RANDOM_SEED},
        scaling_config=ScalingConfig(
            num_workers=2,
            use_gpu=False,
            resources_per_worker={"CPU": 1},
        ),
        run_config=RunConfig(
            name=f"{run_name}-torch",
            storage_path=storage_path,
            storage_filesystem=storage_filesystem,
        ),
    ).fit()
    if torch_result.checkpoint is None:
        raise RuntimeError("Ray Train did not produce a PyTorch checkpoint.")
    with torch_result.checkpoint.as_directory() as checkpoint_dir:
        restored_state = torch.load(
            f"{checkpoint_dir}/model.pt",
            map_location="cpu",
            weights_only=True,
        )
    if not restored_state:
        raise RuntimeError("Restored PyTorch checkpoint is empty.")
    expected_prior_checkpoint = expected_prior_rounds > 0
    if bool(torch_result.metrics["prior_checkpoint_restored"]) != expected_prior_checkpoint:
        raise RuntimeError("PyTorch checkpoint restore state differs from the attempt contract.")

    print(
        json.dumps(
            {
                "deep_learning": {
                    "backend": "ray_train_torch",
                    "checkpoint_restored": True,
                    "framework": "pytorch",
                    "prior_checkpoint_restored": expected_prior_checkpoint,
                    "storage_scheme": "s3",
                    "train_loss": float(torch_result.metrics["train_loss"]),
                    "workers": 2,
                },
                "distributed_native_cpu": {
                    "backend": "ray_train_xgboost",
                    "checkpoint_restored": True,
                    "num_boosted_rounds": booster.num_boosted_rounds(),
                    "prior_checkpoint_restored": expected_prior_rounds > 0,
                    "storage_scheme": "s3",
                    "train_logloss": float(result.metrics["train-logloss"]),
                    "workers": 2,
                },
                "incremental": {
                    "accuracy": incremental_accuracy,
                    "backend": "incremental_sklearn",
                    "checkpoint_digest": checkpoint_digest,
                    "checkpoint_restored": True,
                    "state_owners": 1,
                },
                "ordinary": {
                    "accuracy": ordinary_accuracy,
                    "backend": "single_process_sklearn",
                    "workers": 1,
                },
                "ray_node_count": len([node for node in ray.nodes() if node.get("Alive")]),
                "status": "passed",
            },
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    ray.init(address="auto")
    main()
