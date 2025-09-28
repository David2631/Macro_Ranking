import os
import subprocess


def test_pipeline_runs_with_fixtures(tmp_path):
    # call the fixture runner with the example-config-fixtures.yaml
    cfg = os.path.abspath("example-config-fixtures.yaml")
    # ensure output folder
    out_dir = os.path.abspath("output")
    if not os.path.isdir(out_dir):
        os.makedirs(out_dir, exist_ok=True)
    # run the script
    res = subprocess.run(
        ["python", "scripts/ci_fixture_run.py", cfg], capture_output=True, text=True
    )
    print(res.stdout)
    # the runner should write the configured output file
    out_file = os.path.abspath("output/fixture_macro_ranking.xlsx")
    assert os.path.exists(out_file)
