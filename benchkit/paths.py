from pathlib import Path


ROOT = Path(__file__).resolve().parent.parent
CANONICAL_DIR = ROOT / "canonical"
RESULTS_DIR = ROOT / "results"
FINAL_RESULTS_DIR = RESULTS_DIR / "final"
FINAL_TRACES_DIR = FINAL_RESULTS_DIR / "traces"
CALIBRATION_DIR = RESULTS_DIR / "calibration"
REPORT_DIR = RESULTS_DIR / "report"
REPORT_FIGURES_DIR = REPORT_DIR / "figures"
MANIFEST_PATH = CANONICAL_DIR / "experiment_manifest.yaml"
RESULT_SCHEMA_PATH = CANONICAL_DIR / "result_schema_v3_5_0.json"
TRACE_SCHEMA_PATH = CANONICAL_DIR / "trace_record_schema.json"
STUB_CALIBRATION_SCHEMA_PATH = CANONICAL_DIR / "stub_calibration_schema.json"
