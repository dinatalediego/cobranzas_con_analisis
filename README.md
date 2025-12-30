# Cygnus Behavior DS (4 modelos) — Risk • Time • Cashflow • Clustering

Run:
python -m cygnus_behavior_ds run --data-dir data --out artifacts

# Cygnus Cobranzas — Artifact Pipeline (FINAL)

✅ Incluye:
- Tu **SQL embebida** (`sql_minutas_base.sql`) con columnas:
  - `codigo_proforma`, `codigo_unidad`, `documento_cliente`, `proyecto`, `asesor`, `cliente`,
  - `precio_venta_depa_soles`, `precio_estacionamiento_proforma`, `precio_deposito_proforma`,
  - `tipo_compra`, `precio_total_venta`, etc.
- ETL end-to-end: Extract (Redshift + Excel) → Transform → Report → Snapshots
- Artefactos por etapa: `stage_*.md/json`
- GitHub Actions diario + email (igual al patrón que te salió verde)

## Inputs
- Excel: `data/inputs/pagos.xlsx` (simulado, puedes reemplazar por tu real)
  - Llave recomendada: `codigo_proforma`
  - Si `tipo_item/codigo_item` vienen vacíos, el pago se considera a nivel **PROFORMA**
  - Si vienen llenos, se calcula además un reporte **por item** (depa/estac/deposito)

## Ejecutar local
1) `.env` con:
```
REDSHIFT_HOST=...
REDSHIFT_PORT=5439
REDSHIFT_DB=...
REDSHIFT_USER=...
REDSHIFT_PASSWORD=...
```
2) Instalar deps:
```
pip install -r requirements.txt
```
3) Correr:
```
python -m src.pipeline --excel data/inputs/pagos.xlsx --out artifacts --snapshot
```

## Outputs
- `artifacts/extract_ventas.csv`
- `artifacts/extract_pagos.csv`
- `artifacts/cobranzas_report.csv`  (deuda por proforma)
- `artifacts/cobranzas_items_report.csv` (deuda por item, si aplica)
- `artifacts/cobranzas_summary.md`
- `artifacts/stage_*.md/json`
- `artifacts/snapshots/cobranzas_YYYY-MM-DD.csv`
