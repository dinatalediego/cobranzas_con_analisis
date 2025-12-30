import pandas as pd

def compute_cobranzas(df_xcobrar: pd.DataFrame, df_pagos: pd.DataFrame) -> pd.DataFrame:
    # Esperado en df_xcobrar: cliente, unidad, tipo_item (DEP/EST), total_por_cobrar, fecha_vencimiento (opcional)
    # Pagos: cliente, unidad, monto, fecha_pago

    pagos_agg = (df_pagos
        .groupby(["cliente", "unidad"], as_index=False)
        .agg(total_pagado=("monto", "sum"),
             ultima_fecha_pago=("fecha_pago", "max"))
    )

    out = df_xcobrar.merge(pagos_agg, on=["cliente", "unidad"], how="left")
    out["total_pagado"] = out["total_pagado"].fillna(0.0)
    out["saldo_pendiente"] = out["total_por_cobrar"] - out["total_pagado"]

    # Mora si hay vencimiento
    if "fecha_vencimiento" in out.columns:
        out["fecha_vencimiento"] = pd.to_datetime(out["fecha_vencimiento"], errors="coerce")
        hoy = pd.Timestamp.utcnow().normalize()
        out["dias_mora"] = (hoy - out["fecha_vencimiento"]).dt.days
        out.loc[out["dias_mora"] < 0, "dias_mora"] = 0
    else:
        out["dias_mora"] = None

    return out
