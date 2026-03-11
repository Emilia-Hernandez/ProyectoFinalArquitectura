from app.pipeline.producer import _simulate_stream


def test_simulate_stream_has_expected_fields():
    stream = _simulate_stream(symbol="IBM", rate_per_second=1)
    sample = next(stream)

    expected = {"event_time", "symbol", "open", "high", "low", "close", "volume", "source"}
    assert expected.issubset(sample.keys())
    assert sample["symbol"] == "IBM"
    assert sample["source"] == "simulated"
