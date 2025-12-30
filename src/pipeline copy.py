from __future__ import annotations
import argparse
from pathlib import Path
from datetime import datetime
import pandas as pd

from .anonymize import anon_client, anon_unit

from .io_redshift import read_sql
from .stages import StageResult, write_stage_artifact, save_snapshot

DEFAULT_SQL_PATH = Path(__file__).resolve().parents[1] / "sql_minutas_base.sql"

def ts():
    return datetime.utcnow().isoformat(timespec="seconds") + "Z"

def extract_minutas(sql_path: Path, out_dir: Path) -> pd.DataFrame:
    started = ts()
    query = sql_path.read_text(encoding="utf-8")
    df = read_sql(query)
    finished = ts()

    metrics = {
        "rows": int(len(df)),
        "columns": list(df.columns),
        "sql_path": str(sql_path),
    }
    write_stage_artifact(out_dir, StageResult("extract_redshift_ventas", started, finished, metrics))
    df.to_csv(out_dir / "extract_ventas.csv", index=False)
    return df

def extract_pagos(excel_path: Path, out_dir: Path) -> pd.DataFrame:
    started = ts()
    pagos = pd.read_excel(excel_path, sheet_name="pagos")
    pagos.columns = [c.strip().lower() for c in pagos.columns]

    required = {"codigo_proforma", "monto_pagado"}
    missing = sorted(list(required - set(pagos.columns)))

    finished = ts()
    metrics = {
        "rows": int(len(pagos)),
        "missing_required_columns": missing,
        "excel_path": str(excel_path),
    }
    write_stage_artifact(out_dir, StageResult("extract_excel_pagos", started, finished, metrics))
    pagos.to_csv(out_dir / "extract_pagos.csv", index=False)

    if missing:
        raise ValueError(f"Excel pagos.xlsx no tiene columnas requeridas: {missing}")
    return pagos

def _agg_pagos_proforma(pagos: pd.DataFrame) -> pd.DataFrame:
    # pagos a nivel proforma = filas donde tipo_item/codigo_item están vacíos (o no existen)
    if "tipo_item" in pagos.columns and "codigo_item" in pagos.columns:
        mask = (pagos["tipo_item"].fillna("").astype(str).str.strip() == "") & (pagos["codigo_item"].fillna("").astype(str).str.strip() == "")
        p2 = pagos.loc[mask].copy()
    else:
        p2 = pagos.copy()
    if "fecha_pago" in p2.columns:
        p2["fecha_pago"] = pd.to_datetime(p2["fecha_pago"], errors="coerce")
    return (p2.groupby(["codigo_proforma"], as_index=False)
              .agg(total_pagado=("monto_pagado", "sum"),
                   n_pagos=("monto_pagado", "count"),
                   fecha_ultimo_pago=("fecha_pago", "max")))

def _agg_pagos_item(pagos: pd.DataFrame) -> pd.DataFrame | None:
    if not {"tipo_item","codigo_item"}.issubset(pagos.columns):
        return None
    p2 = pagos.copy()
    p2["tipo_item"] = p2["tipo_item"].fillna("").astype(str).str.strip().str.lower()
    p2["codigo_item"] = p2["codigo_item"].fillna("").astype(str).str.strip()
    # solo filas con item
    p2 = p2[(p2["tipo_item"] != "") & (p2["codigo_item"] != "")]
    if p2.empty:
        return None
    if "fecha_pago" in p2.columns:
        p2["fecha_pago"] = pd.to_datetime(p2["fecha_pago"], errors="coerce")
    return (p2.groupby(["codigo_proforma","tipo_item","codigo_item"], as_index=False)
              .agg(total_pagado=("monto_pagado","sum"),
                   n_pagos=("monto_pagado","count"),
                   fecha_ultimo_pago=("fecha_pago","max")))

def transform_cobranzas(ventas: pd.DataFrame, pagos: pd.DataFrame, out_dir: Path) -> pd.DataFrame:
    started = ts()

    # 1) pagos por proforma
    pagos_pf = _agg_pagos_proforma(pagos)

    df = ventas.merge(pagos_pf, on="codigo_proforma", how="left")
    df["total_pagado"] = df["total_pagado"].fillna(0.0)
    df["n_pagos"] = df["n_pagos"].fillna(0).astype(int)

    # 2) total pactado = precio_total_venta (columna exacta de tu SQL)
    df["precio_total_venta"] = pd.to_numeric(df.get("precio_total_venta"), errors="coerce").fillna(0.0)

    # 3) deuda proxy
    df["deuda_pendiente"] = (df["precio_total_venta"] - df["total_pagado"]).clip(lower=0.0)
    df["avance_pct"] = df.apply(lambda r: (r["total_pagado"]/r["precio_total_venta"]) if r["precio_total_venta"] else 0.0, axis=1)

    # 4) prioridad
    df["prioridad"] = pd.cut(
        df["deuda_pendiente"],
        bins=[-0.1, 0, 5000, 20000, 1e18],
        labels=["sin_deuda", "baja", "media", "alta"]
    )

    # 5) item-level (si hay pagos por item): departamento/estacionamiento/deposito
    pagos_item = _agg_pagos_item(pagos)
    if pagos_item is not None:
        items = []

        def _safe_num(x):
            try:
                return float(x) if x is not None else 0.0
            except Exception:
                return 0.0

        for _, r in df.iterrows():
            pf = r.get("codigo_proforma")
            cliente = r.get("cliente", "")
            proyecto = r.get("proyecto", "")
            asesor = r.get("asesor", "")

            # Departamento
            items.append({
                "codigo_proforma": pf,
                "tipo_item": "departamento",
                "codigo_item": r.get("codigo_unidad", ""),
                "precio_item": _safe_num(r.get("precio_venta_depa_soles", 0)),
                "cliente": cliente, "proyecto": proyecto, "asesor": asesor
            })

            # Estacionamiento
            if pd.notna(r.get("codigo_estacionamiento_proforma")) and str(r.get("codigo_estacionamiento_proforma")).strip() != "":
                items.append({
                    "codigo_proforma": pf,
                    "tipo_item": "estacionamiento",
                    "codigo_item": str(r.get("codigo_estacionamiento_proforma")),
                    "precio_item": _safe_num(r.get("precio_estacionamiento_proforma", 0)),
                    "cliente": cliente, "proyecto": proyecto, "asesor": asesor
                })

            # Depósito
            if pd.notna(r.get("codigo_deposito_proforma")) and str(r.get("codigo_deposito_proforma")).strip() != "":
                items.append({
                    "codigo_proforma": pf,
                    "tipo_item": "deposito",
                    "codigo_item": str(r.get("codigo_deposito_proforma")),
                    "precio_item": _safe_num(r.get("precio_deposito_proforma", 0)),
                    "cliente": cliente, "proyecto": proyecto, "asesor": asesor
                })

        items_df = pd.DataFrame(items)
        items_df = items_df.merge(pagos_item, on=["codigo_proforma","tipo_item","codigo_item"], how="left")
        items_df["total_pagado"] = items_df["total_pagado"].fillna(0.0)
        items_df["n_pagos"] = items_df["n_pagos"].fillna(0).astype(int)
        items_df["deuda_item"] = (items_df["precio_item"] - items_df["total_pagado"]).clip(lower=0.0)
        items_df["avance_item_pct"] = items_df.apply(lambda x: (x["total_pagado"]/x["precio_item"]) if x["precio_item"] else 0.0, axis=1)

        items_df.to_csv(out_dir / "cobranzas_items_report.csv", index=False)

    finished = ts()
    metrics = {
        "rows_out": int(len(df)),
        "deuda_total": float(df["deuda_pendiente"].sum()),
        "ventas_con_deuda": int((df["deuda_pendiente"] > 0).sum()),
        "top_deuda_max": float(df["deuda_pendiente"].max()) if len(df) else 0.0,
        "item_report_generated": bool((out_dir / "cobranzas_items_report.csv").exists()),
    }
    write_stage_artifact(out_dir, StageResult("transform_cobranzas", started, finished, metrics))

    df.to_csv(out_dir / "cobranzas_report.csv", index=False)
    return df

def build_summary(df: pd.DataFrame, out_dir: Path) -> None:
    started = ts()
    total = float(df["deuda_pendiente"].sum())
    n = int((df["deuda_pendiente"] > 0).sum())

    top = df.sort_values("deuda_pendiente", ascending=False).head(10)

    md = []
    md.append(f"- **Deuda pendiente total (proxy):** {total:,.2f}")
    md.append(f"- **# ventas con deuda:** {n}")
    md.append("")
    md.append("**Top 10 deudas (proxy):**")
    md.append("")
    md.append("| Proforma | Proyecto | Cliente | Asesor | Deuda | Pagado | Total Venta | Avance | Tipo compra |")
    md.append("|---|---|---|---|---:|---:|---:|---:|---|")
    for _, r in top.iterrows():
        md.append(
            f"| {r.get('codigo_proforma','')} | {r.get('proyecto','')} | {r.get('cliente','')} | {r.get('asesor','')} | {r.get('deuda_pendiente',0):,.2f} | {r.get('total_pagado',0):,.2f} | {r.get('precio_total_venta',0):,.2f} | {r.get('avance_pct',0)*100:,.1f}% | {r.get('tipo_compra','')} |"
        )

    out_dir.mkdir(parents=True, exist_ok=True)
    (out_dir / "cobranzas_summary.md").write_text("\n".join(md), encoding="utf-8")

    finished = ts()
    metrics = {"summary_path": str(out_dir / "cobranzas_summary.md")}
    write_stage_artifact(out_dir, StageResult("report_summary", started, finished, metrics))

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--excel", required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--sql", default=str(DEFAULT_SQL_PATH))
    ap.add_argument("--snapshot", action="store_true")
    args = ap.parse_args()

    out_dir = Path(args.out)
    out_dir.mkdir(parents=True, exist_ok=True)

    ventas = extract_minutas(Path(args.sql), out_dir)
    pagos = extract_pagos(Path(args.excel), out_dir)

    cobr = transform_cobranzas(ventas, pagos, out_dir)
    build_summary(cobr, out_dir)

    if args.snapshot:
        snap = save_snapshot(cobr, out_dir / "snapshots", "cobranzas")
        print(f"Snapshot saved: {snap}")

    print("OK: pipeline completo. Revisa artifacts/")

if __name__ == "__main__":
    main()
