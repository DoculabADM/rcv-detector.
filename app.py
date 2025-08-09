# app.py
import io
import re
import zipfile
from pathlib import Path

import pandas as pd
import streamlit as st

# ================== Config básica (se renderiza altiro) ==================
st.set_page_config(page_title="RCV Detector", page_icon="🧭", layout="wide")
st.title("🧭 Detección de nuevos RUT (Clientes / Proveedores)")

st.caption("Sube tu **listado base** (CSV con formato especial) y varios **RCV** (CSV o ZIP). "
           "Luego presiona **Detectar nuevos**. La app no procesa nada hasta que aprietes el botón 😉")

# ================== Utilidades ==================
RUT_PAT = re.compile(r"^\s*\d{7,9}-[\dkK]\s*$")
PERIODO_RE = re.compile(r"(\d{6})(?=\.csv$)", re.IGNORECASE)


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


def parse_tipo_periodo_from_name(name: str):
    up = name.upper()
    tipo = "COMPRA" if "COMPRA" in up else ("VENTA" if "VENTA" in up else None)
    per_m = PERIODO_RE.search(name)
    periodo = per_m.group(1) if per_m else None
    return tipo, periodo


def choose_rut_col(df: pd.DataFrame, aliases: list[str]) -> str:
    # 1) por alias normalizado
    norm_map = {re.sub(r"\s+", " ", c.strip().upper()): c for c in df.columns}
    for a in aliases:
        key = re.sub(r"\s+", " ", a.strip().upper())
        if key in norm_map:
            return norm_map[key]
    # 2) por contenido (la que más parece RUT)
    best_col, best_hits = None, -1
    for c in df.columns:
        hits = df[c].astype(str).str.match(RUT_PAT, na=False).sum()
        if hits > best_hits:
            best_col, best_hits = c, hits
    return best_col or (aliases[0] if aliases else df.columns[0])


def choose_rs_col(df: pd.DataFrame, aliases: list[str], rut_col: str) -> str:
    # 1) por alias
    norm_map = {re.sub(r"\s+", " ", c.strip().upper()): c for c in df.columns}
    for a in aliases:
        key = re.sub(r"\s+", " ", a.strip().upper())
        if key in norm_map:
            return norm_map[key]
    # 2) por “textualidad” (más letras y longitud media)
    best_col, best_score = None, -1.0
    for c in df.columns:
        if c == rut_col:
            continue
        s = df[c].astype(str)
        letters = s.str.count(r"[A-Za-zÁÉÍÓÚÜÑáéíóúüñ]").fillna(0)
        lengths = s.str.len().fillna(0)
        score = letters.mean() + lengths.mean() * 0.05
        if score > best_score:
            best_col, best_score = c, score
    return best_col or (aliases[0] if aliases else df.columns[0])

# ================== Caching ==================


@st.cache_data(show_spinner=False)
def parse_listado_raro_from_bytes(raw_bytes: bytes) -> pd.DataFrame:
    """Parsea el listado (coma inicial + comillas x2) desde bytes → DataFrame RUT,NOMBRE,CLIENTE,PROVEEDOR."""
    # decodificación
    try:
        text = raw_bytes.decode("utf-8")
    except UnicodeDecodeError:
        text = raw_bytes.decode("latin-1", errors="ignore")

    rows = []
    for ln in text.splitlines():
        ln = ln.strip().lstrip("\ufeff")
        if not ln:
            continue
        if ln.startswith(","):
            ln = ln[1:]
        if ln.startswith('"') and ln.endswith('"'):
            ln = ln[1:-1]
        parts = [strip_outer_quotes(p) for p in ln.split('","')]
        rows.append(parts)

    if not rows:
        return pd.DataFrame(columns=["RUT", "NOMBRE", "CLIENTE", "PROVEEDOR"])

    header = [strip_outer_quotes(h).upper() for h in rows[0]]
    data = rows[1:]
    if header and header[0] == "":
        header = header[1:]
        data = [r[1:] for r in data]

    df = pd.DataFrame(data, columns=header)

    keep = ["RUT", "NOMBRE", "CLIENTE", "PROVEEDOR"]
    for k in keep:
        if k not in df.columns:
            df[k] = ""
    df = df[keep].copy()

    for c in df.columns:
        df[c] = df[c].astype(str).str.strip()

    df = df[df["RUT"].ne("-") & df["RUT"].ne("")].copy()
    df["RUT"] = df["RUT"].map(norm_rut)
    df["CLIENTE"] = df["CLIENTE"].str.upper()
    df["PROVEEDOR"] = df["PROVEEDOR"].str.upper()
    return df.reset_index(drop=True)


@st.cache_data(show_spinner=False)
def leer_rcv_from_bytes(name: str, raw_bytes: bytes) -> pd.DataFrame:
    """Lee RCV separado por ';' desde bytes, normaliza headers/valores."""
    try:
        df = pd.read_csv(io.BytesIO(raw_bytes), sep=";",
                         dtype=str, encoding="utf-8", engine="python")
    except UnicodeDecodeError:
        df = pd.read_csv(io.BytesIO(raw_bytes), sep=";",
                         dtype=str, encoding="latin-1", engine="python")
    df.columns = [c.strip() for c in df.columns]
    for c in df.columns:
        df[c] = df[c].astype(str).str.strip()
    return df


def to_xlsx_bytes(dfs: dict[str, pd.DataFrame]) -> bytes:
    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine="xlsxwriter") as writer:
        for sheet, d in dfs.items():
            # Evita hojas vacías sin columnas
            if d is None or d.empty:
                d = pd.DataFrame(columns=["RUT", "RAZON_SOCIAL", "PERIODO"])
            d.to_excel(writer, index=False, sheet_name=(sheet or "Sheet")[:31])
    buf.seek(0)
    return buf.getvalue()


# ================== UI: uploaders ==================
with st.sidebar:
    st.header("Carga de archivos")
    listado_file = st.file_uploader("Listado base (CSV 'raro')", type=[
                                    "csv"], accept_multiple_files=False)
    rcv_files = st.file_uploader("RCV (CSV o ZIP, puedes subir varios)", type=[
                                 "csv", "zip"], accept_multiple_files=True)
    procesar = st.button("🚀 Detectar nuevos")

st.info("1) Sube **listado base**. 2) Sube **RCV** (CSV o ZIP). 3) Clic en **Detectar nuevos**.", icon="ℹ️")

# ================== Procesamiento bajo demanda ==================
if procesar:
    try:
        if not listado_file:
            st.error("Debes subir el listado base.")
            st.stop()

        # 1) Parsear listado
        listado_bytes = listado_file.read()
        listado_df = parse_listado_raro_from_bytes(listado_bytes)
        if listado_df.empty or "RUT" not in listado_df.columns:
            st.error("No se pudo interpretar el listado. Revisa el archivo.")
            st.stop()

        ruts_proveedores_sis = set(
            listado_df.loc[listado_df["PROVEEDOR"] == "SI", "RUT"])
        ruts_clientes_sis = set(
            listado_df.loc[listado_df["CLIENTE"] == "SI", "RUT"])

        # 2) Cargar RCVs
        rcv_items: list[tuple[str, pd.DataFrame]] = []
        for f in rcv_files or []:
            name = f.name
            data = f.read()
            if name.lower().endswith(".zip"):
                try:
                    with zipfile.ZipFile(io.BytesIO(data)) as z:
                        for zi in z.infolist():
                            if zi.filename.lower().endswith(".csv") and Path(zi.filename).name.upper().startswith("RCV_"):
                                raw = z.read(zi.filename)
                                df = leer_rcv_from_bytes(zi.filename, raw)
                                rcv_items.append((Path(zi.filename).name, df))
                except zipfile.BadZipFile:
                    st.error(f"El archivo {name} no es un ZIP válido.")
            else:
                df = leer_rcv_from_bytes(name, data)
                rcv_items.append((name, df))

        if not rcv_items:
            st.error("No se detectaron RCV válidos en lo que subiste.")
            st.stop()

        # 3) Detectar nuevos por archivo
        nuevos_proveedores_rows = []
        nuevos_clientes_rows = []

        with st.spinner("Procesando RCV…"):
            for name, df in rcv_items:
                tipo, periodo = parse_tipo_periodo_from_name(name)
                if tipo not in {"COMPRA", "VENTA"} or not periodo:
                    # ignorar archivos que no calzan el patrón
                    continue

                if tipo == "COMPRA":
                    rut_aliases = ["RUT Proveedor", "RUT_PROVEEDOR",
                                   "RUT PROVEEDOR", "RUTPROVEEDOR"]
                    rs_aliases = ["Razón Social", "RAZON SOCIAL",
                                  "Razon Social", "RAZON_SOCIAL"]
                else:
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
                tmp["RAZON_SOCIAL"] = tmp["RAZON_SOCIAL"].astype(
                    str).str.strip()
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
                        nuevos_clientes_rows.extend(
                            nuevos.to_dict(orient="records"))

        # 4) Tablas y descargas
        cols = ["RUT", "RAZON_SOCIAL", "PERIODO"]
        prov_df = pd.DataFrame(nuevos_proveedores_rows,
                               columns=cols).dropna(how="all")
        cli_df = pd.DataFrame(nuevos_clientes_rows,
                              columns=cols).dropna(how="all")

        if not prov_df.empty:
            prov_df["RUT"] = prov_df["RUT"].map(norm_rut)
            prov_df = prov_df[prov_df["RUT"] != ""].drop_duplicates(
                ["RUT", "PERIODO"]).sort_values(["PERIODO", "RUT"])
        if not cli_df.empty:
            cli_df["RUT"] = cli_df["RUT"].map(norm_rut)
            cli_df = cli_df[cli_df["RUT"] != ""].drop_duplicates(
                ["RUT", "PERIODO"]).sort_values(["PERIODO", "RUT"])

        c1, c2 = st.columns(2)
        with c1:
            st.subheader("Nuevos proveedores")
            if prov_df.empty:
                st.success("No se detectan nuevos proveedores")
            else:
                st.dataframe(prov_df, use_container_width=True,
                             hide_index=True)
                st.download_button(
                    "⬇️ Descargar nuevos_proveedores.csv",
                    data=prov_df.to_csv(index=False).encode("utf-8-sig"),
                    file_name="nuevos_proveedores.csv",
                    mime="text/csv",
                )
        with c2:
            st.subheader("Nuevos clientes")
            if cli_df.empty:
                st.success("No se detectan nuevos clientes")
            else:
                st.dataframe(cli_df, use_container_width=True, hide_index=True)
                st.download_button(
                    "⬇️ Descargar nuevos_clientes.csv",
                    data=cli_df.to_csv(index=False).encode("utf-8-sig"),
                    file_name="nuevos_clientes.csv",
                    mime="text/csv",
                )

        if not prov_df.empty or not cli_df.empty:
            xlsx_bytes = to_xlsx_bytes({
                "Nuevos_Proveedores": prov_df,
                "Nuevos_Clientes": cli_df,
            })
            st.download_button(
                "⬇️ Descargar consolidado.xlsx",
                data=xlsx_bytes,
                file_name="nuevos_consolidados.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            )

    except Exception as e:
        st.error("Ocurrió un error durante el procesamiento:")
        st.exception(e)
else:
    st.stop()  # No hace nada más hasta que presiones el botón
