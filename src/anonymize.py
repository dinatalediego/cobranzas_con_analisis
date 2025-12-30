from __future__ import annotations
import hashlib

def stable_hash(value: str, salt: str) -> str:
    raw = f"{salt}|{value}".encode("utf-8")
    return hashlib.sha256(raw).hexdigest()[:24]  # corto pero estable

def anon_client(nombres: str, apellidos: str, documento: str | None, salt: str) -> str:
    # usa documento si existe (mejor unicidad), sino nombre completo
    key = documento.strip() if documento and str(documento).strip() else f"{nombres} {apellidos}".strip()
    return stable_hash(key, salt)

def anon_unit(codigo_proforma: str, salt: str) -> str:
    return stable_hash(str(codigo_proforma), salt)
