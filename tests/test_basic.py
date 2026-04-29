from pathlib import Path

def test_bundle_file_exists():
    assert Path("databricks.yml").exists()

def test_jobs_file_exists():
    assert Path("resources/jobs.yml").exists()