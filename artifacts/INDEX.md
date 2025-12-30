# 📦 Cygnus – Artefactos de Cobranzas

Fecha de generación: 2025-12-30 05:34

## 🧱 Estructura y significado

### 1️⃣ Extract (fuentes)
- extract_ventas.csv  
  Ventas desde Redshift (SQL oro: depa + estacionamiento + depósito + precio_total_venta)

- extract_pagos.csv  
  Pagos cargados desde Excel local (fuente de cobranzas)

### 2️⃣ Transform (modelo cobranzas)
- cobranzas_report.csv  
  Tabla final por proforma:
  - precio_total_venta  
  - total_pagado  
  - deuda_pendiente  
  - avance_pct  

- cobranzas_items_report.csv (si aplica)  
  Deuda por ítem: departamento / estacionamiento / depósito

### 3️⃣ Report (ejecutivo)
- cobranzas_summary.md  
  Resumen enviado por email y usado para comité

### 4️⃣ Auditoría por etapa
- stage_extract_redshift_ventas.md / .json
- stage_extract_excel_pagos.md / .json
- stage_transform_cobranzas.md / .json
- stage_report_summary.md / .json

### 5️⃣ Histórico
- snapshots/  
  Fotografías diarias para comparar evolución de deuda

## 🧠 Uso recomendado (pensando como dueño)
1. Revisar cobranzas_summary.md
2. Ir a cobranzas_report.csv para priorizar cobranza
3. Usar snapshots/ para ver nuevas deudas vs pagos
4. Auditar pipeline con stage_*.md

---
Este índice es generado automáticamente por el pipeline.
