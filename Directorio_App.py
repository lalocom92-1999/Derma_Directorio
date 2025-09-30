# Directorio_App.py
# App local (Streamlit) para buscar clientes y ver: Estatus, Días desde última venta, Último vendedor

from datetime import datetime, timezone
from pathlib import Path
import io
import pandas as pd
import streamlit as st

# ======================== CONFIG =========================
VENTAS_FILE = Path("Base de clientes con historico_RAW.xlsx")  # <-- tu archivo real (en misma carpeta)
VENTAS_SHEET = "Historico_de_ventas"                           # <-- tu hoja real
CATALOGO_FILE = Path("Catalogo_clientes.xlsx")                 # opcional
CATALOGO_SHEET = "DirectorioClientes"                          # opcional
DIAS_EN_USO = 90

COL_CLIENTE = "Cliente"
COL_VENDEDOR = "Nombredecontacto"  # “último vendedor”

# Elige las fechas que definen que la venta ocurrió (de tu base):
FECHAS_COLS = [
    "Fechadepedido",
    "Fechadeenvíosolicitada",
    "Fechadevencimiento",
    "Fechaenviada",
    "Fechadelafactura",
    "FechadePago",
    "FECHA REAL DEPAGO",
    "fecha de envio",
]

# Campos del catálogo (si existe)
CAT_LLAVE = "Cliente"
CAT_CIUDAD = "Ciudad"
CAT_EMAIL  = "Correo electrónico"
CAT_PAIS   = "País"
CAT_TEL    = "Teléfono"

# ===================== FUNCIONES =========================
def _to_datetime(s: pd.Series) -> pd.Series:
    return pd.to_datetime(s, errors="coerce", infer_datetime_format=True)

@st.cache_data(show_spinner=True)
def cargar_ventas(path: Path, sheet: str) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"No encuentro el archivo: {path.resolve()}")
    df = pd.read_excel(path, sheet_name=sheet, dtype={COL_CLIENTE: "string"})
    # normalizar fechas
    existentes = []
    for c in FECHAS_COLS:
        if c in df.columns:
            df[c] = _to_datetime(df[c])
            existentes.append(c)
    if not existentes:
        raise ValueError("No se encontraron columnas de fecha válidas. Revisa FECHAS_COLS.")
    df["FechaVenta_Fila"] = df[existentes].max(axis=1, skipna=True)
    return df

def calcular_directorio(df_ventas: pd.DataFrame) -> pd.DataFrame:
    df = df_ventas.copy()
    df = df[df[COL_CLIENTE].notna()].copy()
    df[COL_CLIENTE] = df[COL_CLIENTE].astype("string").str.strip()

    agg = df.groupby(COL_CLIENTE, as_index=False).agg(UltimaFechaVenta=("FechaVenta_Fila", "max"))

    hoy = pd.Timestamp(datetime.now(timezone.utc).date())
    agg["Dias_desde_ultima_venta"] = (hoy - agg["UltimaFechaVenta"]).dt.days

    def _estatus(row):
        f = row["UltimaFechaVenta"]
        if pd.isna(f):
            return "Disponible"
        return "En uso" if row["Dias_desde_ultima_venta"] < DIAS_EN_USO else "Disponible"

    agg["Estatus"] = agg.apply(_estatus, axis=1)

    # último vendedor a partir de la fila que coincide con la UltimaFechaVenta
    join_cols = [COL_CLIENTE, "FechaVenta_Fila", COL_VENDEDOR]
    join_cols = [c for c in join_cols if c in df.columns]
    df_join = df[join_cols].copy()
    dir_df = (
        agg.merge(df_join, left_on=[COL_CLIENTE, "UltimaFechaVenta"],
                  right_on=[COL_CLIENTE, "FechaVenta_Fila"], how="left")
           .sort_values([COL_CLIENTE, "FechaVenta_Fila"], ascending=[True, False])
           .drop_duplicates(subset=[COL_CLIENTE], keep="first")
           .rename(columns={COL_VENDEDOR: "UltimoVendedor"})
    )

    out = dir_df[[COL_CLIENTE, "Estatus", "Dias_desde_ultima_venta", "UltimoVendedor", "UltimaFechaVenta"]].copy()
    return out.sort_values(["Estatus", "Dias_desde_ultima_venta"], ascending=[True, True]).reset_index(drop=True)

@st.cache_data(show_spinner=False)
def cargar_catalogo(path: Path, sheet: str) -> pd.DataFrame | None:
    if not path.exists():
        return None
    cat = pd.read_excel(path, sheet_name=sheet)
    if CAT_LLAVE not in cat.columns:
        return None
    cat[CAT_LLAVE] = cat[CAT_LLAVE].astype("string").str.strip()
    cols = [c for c in [CAT_LLAVE, CAT_CIUDAD, CAT_EMAIL, CAT_PAIS, CAT_TEL] if c in cat.columns]
    return cat[cols].copy()

def merge_catalogo(directorio: pd.DataFrame, cat: pd.DataFrame | None) -> pd.DataFrame:
    if cat is None:
        return directorio
    out = directorio.merge(cat, left_on=COL_CLIENTE, right_on=CAT_LLAVE, how="left")
    orden = [
        COL_CLIENTE,
        CAT_CIUDAD if CAT_CIUDAD in out.columns else None,
        CAT_EMAIL  if CAT_EMAIL  in out.columns else None,
        CAT_PAIS   if CAT_PAIS   in out.columns else None,
        CAT_TEL    if CAT_TEL    in out.columns else None,
        "Estatus", "Dias_desde_ultima_venta", "UltimoVendedor", "UltimaFechaVenta",
    ]
    orden = [c for c in orden if c and c in out.columns]
    return out[orden].copy()

def to_excel_bytes(df: pd.DataFrame) -> bytes:
    bio = io.BytesIO()
    with pd.ExcelWriter(bio, engine="xlsxwriter", datetime_format="yyyy-mm-dd") as w:
        df.to_excel(w, index=False, sheet_name="Directorio")
        ws = w.sheets["Directorio"]
        ws.set_column(0, max(0, df.shape[1]-1), 22)
    return bio.getvalue()

# ======================== UI ==============================
st.set_page_config(page_title="Directorio de clientes", layout="wide")
st.title("Directorio de clientes")

c1, c2 = st.columns([3, 2])
c1.caption(f"Fuente: **{VENTAS_FILE.name}** › hoja **{VENTAS_SHEET}**")
if CATALOGO_FILE.exists():
    c2.caption(f"Catálogo: **{CATALOGO_FILE.name}** › hoja **{CATALOGO_SHEET}**")
else:
    c2.caption("Catálogo: (opcional)")

# Carga
try:
    ventas = cargar_ventas(VENTAS_FILE, VENTAS_SHEET)
except Exception as e:
    st.error(f"Problema al leer el Excel: {e}")
    st.stop()

catalogo = cargar_catalogo(CATALOGO_FILE, CATALOGO_SHEET)
base = calcular_directorio(ventas)
df = merge_catalogo(base, catalogo)

# Filtros
st.subheader("Buscar y filtrar")
fc1, fc2, fc3, fc4 = st.columns([2,1,1,2])
texto = fc1.text_input("Buscar (Cliente / Ciudad / Correo / País / Teléfono)", "")
estatus = fc2.selectbox("Estatus", ["Todos", "En uso", "Disponible"])
vendedores = ["Todos"] + sorted(df["UltimoVendedor"].dropna().astype(str).unique().tolist())
vendedor = fc3.selectbox("Vendedor", vendedores)
min_d = int(df["Dias_desde_ultima_venta"].min(skipna=True)) if "Dias_desde_ultima_venta" in df else 0
max_d = int(df["Dias_desde_ultima_venta"].max(skipna=True)) if "Dias_desde_ultima_venta" in df else 0
rango = fc4.slider("Rango de días desde última venta", min_value=min(0, min_d), max_value=max(0, max_d),
                   value=(min(0, min_d), max(0, max_d)))

# Aplicar filtros
f = df.copy()

def _contains_any(row, q: str) -> bool:
    q = q.lower()
    campos = [COL_CLIENTE, CAT_CIUDAD, CAT_EMAIL, CAT_PAIS, CAT_TEL]
    for c in campos:
        if c in row and pd.notna(row[c]) and q in str(row[c]).lower():
            return True
    return False

if texto:
    f = f[f.apply(lambda r: _contains_any(r, texto), axis=1)]
if estatus != "Todos":
    f = f[f["Estatus"] == estatus]
if vendedor != "Todos":
    f = f[f["UltimoVendedor"].astype(str) == vendedor]
f = f[(f["Dias_desde_ultima_venta"] >= rango[0]) & (f["Dias_desde_ultima_venta"] <= rango[1])]

st.success(f"Registros: {len(f):,}")
st.dataframe(f, use_container_width=True, height=520)

cdesc1, cdesc2 = st.columns(2)
cdesc1.download_button("Descargar CSV", f.to_csv(index=False).encode("utf-8"),
                       file_name="Directorio_clientes.csv", mime="text/csv")
cdesc2.download_button("Descargar Excel", to_excel_bytes(f),
                       file_name="Directorio_clientes.xlsx",
                       mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")

with st.expander("Notas"):
    st.markdown(
        f"- Estatus: **En uso** si última venta < {DIAS_EN_USO} días; si no, **Disponible**.\n"
        "- Último vendedor se toma de la fila que coincide con la última fecha de venta.\n"
        "- Si tu fecha oficial es solo `Fechadelafactura`, puedes dejar solo esa en FECHAS_COLS.\n"
        "- Coloca los Excel en la **misma carpeta** que este archivo."
    )

