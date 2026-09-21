from types import SimpleNamespace

import numpy as np
import pandas as pd
import pytest
from automl_api.training import analysis_sampling


def test_uniform_stream_sample_is_bounded_and_independent_of_batch_boundaries():
    frame = pd.DataFrame({"row": np.arange(50_000), "label": np.arange(50_000) * 2})

    def chunks(size):
        return (frame.iloc[start : start + size] for start in range(0, len(frame), size))

    small = analysis_sampling.sample_batches(chunks(1_000), max_rows=500, seed=42)
    large = analysis_sampling.sample_batches(chunks(8_000), max_rows=500, seed=42)
    pd.testing.assert_frame_equal(small, large)
    assert len(small) == small.row.nunique() == 500
    assert (small.label == small.row * 2).all()
    assert small.row.min() < 5_000 and small.row.max() > 45_000
    assert small.attrs["source_rows"] == 50_000


@pytest.mark.parametrize("format", ["csv", "parquet", "jsonl", "json"])
def test_sample_source_streams_local_formats_without_whole_object_read(
    tmp_path, monkeypatch, format
):
    frame = pd.DataFrame({"x": np.arange(20_000), "category": ["a", "b"] * 10_000})
    path = tmp_path / f"data.{format}"
    if format == "csv":
        frame.to_csv(path, index=False)
    elif format == "parquet":
        frame.to_parquet(path)
    elif format == "jsonl":
        frame.to_json(path, lines=True, orient="records")
    else:
        frame.to_json(path, orient="records")
    # The driver has no read_bytes method: a whole-object read must fail this test.
    store = SimpleNamespace(
        dataframe_source=lambda uri: SimpleNamespace(path=str(path), filesystem_options={})
    )
    monkeypatch.setattr(analysis_sampling, "get_object_store", lambda: store)
    result = analysis_sampling.sample_source(str(path), path.name, max_rows=200, seed=42)
    assert len(result) == 200
    assert result.attrs["source_rows"] == len(frame)
    assert result.x.max() > 19_000


def test_sample_rejects_invalid_budget():
    with pytest.raises(ValueError, match="positive"):
        analysis_sampling.sample_batches([], max_rows=0, seed=42)
