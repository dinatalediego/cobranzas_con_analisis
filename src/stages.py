from __future__ import annotations
from dataclasses import dataclass
from pathlib import Path
from datetime import datetime, date
import json
import pandas as pd

@dataclass
class StageResult:
    name: str
    started_at: str
    finished_at: str
    metrics: dict

def _ts() -> str:
    return datetime.utcnow().isoformat(timespec="seconds") + "Z"

def write_stage_artifact(out_dir: Path, result: StageResult) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)
    (out_dir / f"stage_{result.name}.json").write_text(
        json.dumps(result.metrics, ensure_ascii=False, indent=2),
        encoding="utf-8"
    )
    md = []
    md.append(f"# Stage: {result.name}")
    md.append("")
    md.append(f"- started_at: `{result.started_at}`")
    md.append(f"- finished_at: `{result.finished_at}`")
    md.append("")
    md.append("## Metrics")
    md.append("")
    for k, v in result.metrics.items():
        md.append(f"- **{k}**: {v}")
    (out_dir / f"stage_{result.name}.md").write_text("\n".join(md), encoding="utf-8")

def save_snapshot(df: pd.DataFrame, out_dir: Path, prefix: str) -> Path:
    out_dir.mkdir(parents=True, exist_ok=True)
    d = date.today().isoformat()
    p = out_dir / f"{prefix}_{d}.csv"
    df.to_csv(p, index=False)
    return p
