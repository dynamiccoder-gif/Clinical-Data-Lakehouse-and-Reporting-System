from pathlib import Path

import pandas as pd

from simulator import start_simulation


def test_simulator_start_chunk_lands_next_chunk(tmp_path):
    source = tmp_path / "encounters.csv"
    target = tmp_path / "raw_ingestion"
    pd.DataFrame({"ID": ["a", "b", "c", "d"], "VALUE": [1, 2, 3, 4]}).to_csv(source, index=False)

    start_simulation(str(source), str(target), size=2, interval_seconds=0, max_chunks=1, start_chunk=1)

    landed_files = list(Path(target).glob("live_encounters*.csv"))
    assert len(landed_files) == 1

    landed = pd.read_csv(landed_files[0])
    assert landed["ID"].tolist() == ["c", "d"]
