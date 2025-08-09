#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import re
from pathlib import Path
import pandas as pd

# ========== EDITA ESTAS RUTAS ==========
LISTADO_PATH = "/Users/ricardojorqueradiaz/Library/Mobile Documents/com~apple~CloudDocs/DOCULAB/Consultora RS/proyecto actualizacion de clientes y proveedores/prueba /lista de proovedores plataforma tributaria/Plataforma tributaria (4).csv"
RCV_DIR = "/Users/ricardojorqueradiaz/Library/Mobile Documents/com~apple~CloudDocs/DOCULAB/Consultora RS/proyecto actualizacion de clientes y proveedores/prueba /RCV"
# ========================================

# -------- Utilidades --------
RUT_RE = re.compile(r"(\d{7,9}-[\dkK])")
PERIODO_RE = re.compile(r"(\d{6})(?=\.csv$)", re.IGNORECASE)
RUT_PAT = re.compile(r"^\s*\d{7,9}-[\dkK]\s*$")


def norm_rut(s: str) -> str:
    s = "" if s is None else str(s).strip()
    if not s:
        return ""
    return s[:-1] + s[-1].upper() if "-" in s and len(s) >= 2 else s


def strip_outer_quotes(s: str) -> str:
    s = str(s).strip().replace('""', '"')
    while len(s) >= 2 and s.startswith('"') and s.endswith('"'):
        s = s[1:-1]
    return s.strip()


def parse_listado_raro(path: str) -> pd.DataFrame:
    """Parsea el listado (coma inicial + comillas x2) → DataFrame con RUT, NOMBRE, CLIENTE, PROVEEDOR."""
    rows = []
    with open(path, "r", encoding="utf-8", errors="ignore") as f:
        for raw in f:
            ln = raw.strip().lstrip("\ufeff")
            if not ln:
                continue
            if ln.startswith(","):
                ln = ln[1:]
            if ln.startswith('"') and ln.endswith('"'):
                ln = ln[1:-1]
            parts = [strip_outer_quotes(p) for p in ln.split('","')]
            rows.append(parts)

    if not rows:
        raise ValueError("Listado vacío o ilegible.")

    header = [strip_outer_quotes(h).upper() for h in rows[0]]
    data = rows[1:]
    if header and header[0] == "":
        header = header[1:]
        data = [r[1:] for r in data]

    df = pd.DataFrame(data, columns=header)

    # asegurar columnas clave
    keep = ["RUT", "NOMBRE", "CLIENTE", "PROVEEDOR"]
    for k in keep:
        if k not in df.columns:
            df[k] = ""
    df = df[keep].copy()

    # limpieza
    for c in df.columns:
        df[c] = df[c].astype(str).str.strip()

    # filtra dummy y vacíos
    df = df[df["RUT"].ne("-") & df["RUT"].ne("")].copy()

    # normaliza
    df["RUT"] = df["RUT"].map(norm_rut)
    df["CLIENTE"] = df["CLIENTE"].str.upper()
    df["PROVEEDOR"] = df["PROVEEDOR"].str.upper()

    return df.reset_index(drop=True)


def leer_rcv(path: Path) -> pd.DataFrame:
    """Lee RCV separado por ';', normaliza headers/valores."""
    try:
        df = pd.read_csv(path, sep=";", dtype=str,
                         encoding="utf-8", engine="python")
    except UnicodeDecodeError:
        df = pd.read_csv(path, sep=";", dtype=str,
                         encoding="latin-1", engine="python")
    df.columns = [c.strip() for c in df.columns]
    for c in df.columns:
        df[c] = df[c].astype(str).str.strip()
    return df


def choose_rut_col(df: pd.DataFrame, aliases: list[str]) -> str:
    """Primero prueba por alias; si no, elige la columna con más matches de patrón RUT."""
    norm_map = {re.sub(r"\s+", " ", c.strip().upper()): c for c in df.columns}
    for a in aliases:
        key = re.sub(r"\s+", " ", a.strip().upper())
        if key in norm_map:
            return norm_map[key]
    best_col, best_hits = None, -1
    for c in df.columns:
        hits = df[c].astype(str).str.match(RUT_PAT, na=False).sum()
        if hits > best_hits:
            best_col, best_hits = c, hits
    return best_col or (aliases[0] if aliases else df.columns[0])


def choose_rs_col(df: pd.DataFrame, aliases: list[str], rut_col: str) -> str:
    """Primero por alias; si no, elige la más 'textual' (más letras y mayor longitud media)."""
    norm_map = {re.sub(r"\s+", " ", c.strip().upper()): c for c in df.columns}
    for a in aliases:
        key = re.sub(r"\s+", " ", a.strip().upper())
        if key in norm_map:
            return norm_map[key]
    best_col, best_score = None, -1.0
    for c in df.columns:
        if c == rut_col:
            continue
        s = df[c].astype(str)
        letters = s.str.count(r"[A-Za-zÁÉÍÓÚÜÑáéíóúüñ]").fillna(0)
        lengths = s.str.len().fillna(0)
        score = letters.mean() + lengths.mean()*0.05
        if score > best_score:
            best_col, best_score = c, score
    return best_col or (aliases[0] if aliases else df.columns[0])


def parse_meta_from_filename(name: str):
    up = name.upper()
    tipo = "COMPRA" if "COMPRA" in up else ("VENTA" if "VENTA" in up else None)
    per_m = PERIODO_RE.search(name)
    periodo = per_m.group(1) if per_m else None
    return tipo, periodo

# -------- Main --------


def main():
    # 1) Listado del sistema
    listado = parse_listado_raro(LISTADO_PATH)
    ruts_proveedores_sis = set(
        listado.loc[listado["PROVEEDOR"] == "SI", "RUT"])
    ruts_clientes_sis = set(listado.loc[listado["CLIENTE"] == "SI", "RUT"])

    # 2) Buscar TODOS los RCV de la carpeta
    rcv_dir = Path(RCV_DIR)
    if not rcv_dir.exists():
        raise FileNotFoundError(f"No existe la carpeta RCV: {rcv_dir}")

    files = sorted([p for p in rcv_dir.glob("*.csv")
                   if p.name.upper().startswith("RCV_")])
    if not files:
        print(f"No se encontraron archivos RCV en {rcv_dir}")
        return

    # Acumuladores
    nuevos_proveedores_rows = []  # dicts con RUT, RAZON_SOCIAL, PERIODO
    nuevos_clientes_rows = []

    for f in files:
        tipo, periodo = parse_meta_from_filename(f.name)
        if tipo not in {"COMPRA", "VENTA"} or not periodo:
            continue

        df = leer_rcv(f)

        if tipo == "COMPRA":
            rut_aliases = ["RUT Proveedor", "RUT_PROVEEDOR",
                           "RUT PROVEEDOR", "RUTPROVEEDOR"]
            rs_aliases = ["Razón Social", "RAZON SOCIAL",
                          "Razon Social", "RAZON_SOCIAL"]
        else:  # VENTA
            rut_aliases = ["Rut cliente", "RUT_CLIENTE",
                           "RUT CLIENTE", "RUTCLIENTE"]
            rs_aliases = ["Razón Social", "RAZON SOCIAL",
                          "Razon Social", "RAZON_SOCIAL"]

        rut_col = choose_rut_col(df, rut_aliases)
        rs_col = choose_rs_col(df, rs_aliases, rut_col)

        if rut_col not in df.columns:
            df[rut_col] = ""
        if rs_col not in df.columns:
            df[rs_col] = ""

        tmp = df[[rut_col, rs_col]].rename(
            columns={rut_col: "RUT", rs_col: "RAZON_SOCIAL"}).copy()
        tmp["RUT"] = tmp["RUT"].map(norm_rut)
        tmp["RAZON_SOCIAL"] = tmp["RAZON_SOCIAL"].astype(str).str.strip()
        tmp = tmp[tmp["RUT"] != ""].drop_duplicates("RUT")
        tmp["PERIODO"] = periodo

        if tipo == "COMPRA":
            nuevos = tmp[~tmp["RUT"].isin(ruts_proveedores_sis)]
            if not nuevos.empty:
                nuevos_proveedores_rows.extend(
                    nuevos.to_dict(orient="records"))
        else:
            nuevos = tmp[~tmp["RUT"].isin(ruts_clientes_sis)]
            if not nuevos.empty:
                nuevos_clientes_rows.extend(nuevos.to_dict(orient="records"))

    # 3) Tablas finales
    cols = ["RUT", "RAZON_SOCIAL", "PERIODO"]
    df_prov = pd.DataFrame(nuevos_proveedores_rows, columns=cols).drop_duplicates(
        ["RUT", "PERIODO"]).sort_values(["PERIODO", "RUT"])
    df_cli = pd.DataFrame(nuevos_clientes_rows,    columns=cols).drop_duplicates(
        ["RUT", "PERIODO"]).sort_values(["PERIODO", "RUT"])

    # 4) Imprimir en tablas
    print("\n=== NUEVOS PROVEEDORES ===")
    if df_prov.empty:
        print("✅ No se detectan nuevos proveedores")
    else:
        print(df_prov.to_string(index=False))

    print("\n=== NUEVOS CLIENTES ===")
    if df_cli.empty:
        print("✅ No se detectan nuevos clientes")
    else:
        print(df_cli.to_string(index=False))

    # (Opcional) Guardar archivos:
    # if not df_prov.empty:
    #     df_prov.to_csv("nuevos_proveedores_con_periodo.csv", index=False, encoding="utf-8-sig")
    # if not df_cli.empty:
    #     df_cli.to_csv("nuevos_clientes_con_periodo.csv", index=False, encoding="utf-8-sig")


if __name__ == "__main__":
    main()
