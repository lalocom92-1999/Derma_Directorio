# Directorio_App.py
# ------------------------------------------------------------------
# Directorio de clientes con Streamlit.
# Lee el Excel desde Google Drive (cuenta de servicio) o local.
# Calcula: Estatus (<90 días = "En uso", si no "Disponible"),
# Días desde última venta y Último vendedor.
# Fusiona catálogo (Ciudad/Correo/País/Provincia/Teléfono) desde
# la hoja "listado completo de clientes al" del mismo Excel.
# ------------------------------------------------------------------

from datetime import datetime, timezone
from pathlib import Path
import io
import pandas as pd
import streamlit as st

# === (A) CONFIGURACIÓN GENERAL ================================================
DIAS_EN_USO = 90
COL_CLIENTE = "Cliente"
COL_VENDEDOR = "Nombredecontacto"  # “último vendedor”

# Fechas que definen que "la venta ocurrió" (ajusta si usas solo una)
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

# Campos del catálogo
CAT_LLAVE  = "Cliente"
CAT_CIUDAD = "Ciudad"
CAT_EMAIL  = "Correo electrónico"
CAT_PAIS   = "País"
CAT_PROV   = "Provincia"
CAT_TEL    = "Teléfono"

# Hoja de catálogo dentro del MISMO Excel y mapeo por letras
CATALOGO_EN_VENTAS_SHEET = "listado completo de clientes al"  # nombre exacto de la hoja
# Letras -> nombre de columna
CAT_MAP = {
    "C": CAT_LLAVE,      # Cliente (llave para unir)
    "A": CAT_CIUDAD,     # Ciudad
    "B": CAT_EMAIL,      # Correo
    "D": CAT_PAIS,       # País
    "E": CAT_PROV,       # Provincia/Estado
    "F": CAT_TEL,        # Teléfono
}

# === (B) FUENTES DE DATOS =====================================================
# Modo de carga: "drive" (recomendado) o "local"
MODO_CARGA = "drive"   # <-- Cambia a "local" si quieres leer desde archivo .xlsx local

# --- Si usas DRIVE (cuenta de servicio) ---------------------------------------
# 🔧 RELLENAR AQUÍ → ID del archivo en Google Drive (el de tu Excel)
GDRIVE_FILE_ID_VENTAS = "1hQZ5t_7soxOdz7TpSqrd71aKVnWkFDES"

# (opcional) Si tuvieras otro archivo de catálogo separado en Drive:
GDRIVE_FILE_ID_CATALOGO = None
SHEET_CATALOGO = "DirectorioClientes"

# Hoja de ventas:
SHEET_VENTAS = "Historico_de_ventas"  # <-- tu hoja real

# Dónde está tu JSON de la cuenta de servicio cuando corres LOCAL en VS Code:
GCP_SA_JSON_PATH = "api-directoriodeclientes-derma-0390d6eea4c2.json"

# --- Si usas LOCAL (archivo .xlsx al lado del .py) ----------------------------
VENTAS_FILE = Path("Base de clientes con historico_RAW.xlsx")
CATALOGO_FILE = Path("Catalogo_clientes.xlsx")  # opcional


# === (C) CARGA DESDE DRIVE (cuenta de servicio) ===============================
def _import_drive_libs():
    from google.oauth2 import service_account
    from googleapiclient.discovery import build
    from googleapiclient.http import MediaIoBaseDownload
    return service_account, build, MediaIoBaseDownload

@st.cache_data(show_spinner=True)
def _drive_service_from_file(json_path: str):
    service_account, build, _ = _import_drive_libs()
    creds = service_account.Credentials.from_service_account_file(
        json_path,
        scopes=["https://www.googleapis.com/auth/drive.readonly"],
    )
    return build("drive", "v3", credentials=creds, cache_discovery=False)

@st.cache_data(show_spinner=True)
def download_drive_file_as_bytes(file_id: str, json_path: str) -> bytes:
    _, _, MediaIoBaseDownload = _import_drive_libs()
    service = _drive_service_from_file(json_path)
    request = service.files().get_media(fileId=file_id)
    buf = io.BytesIO()
    downloader = MediaIoBaseDownload(buf, request)
    done = False
    while not done:
        status, done = downloader.next_chunk()
    buf.seek(0)
    return buf.read()

@st.cache_data(show_spinner=True)
def read_excel_from_drive(file_id: str, sheet_name: str, json_path: str, **pd_kwargs) -> pd.DataFrame:
    """Lee una hoja del Excel de Drive y permite pasar kwargs a pandas.read_excel."""
    binary = download_drive_file_as_bytes(file_id, json_path)
    with io.BytesIO(binary) as bio:
        df = pd.read_excel(bio, sheet_name=sheet_name, **pd_kwargs)
    return df


# === (D) UTILIDADES ===========================================================
def _to_datetime(s: pd.Series) -> pd.Series:
    return pd.to_datetime(s, errors="coerce", infer_datetime_format=True)

def preparar_ventas(df: pd.DataFrame) -> pd.DataFrame:
    existentes = []
    for c in FECHAS_COLS:
        if c in df.columns:
            df[c] = _to_datetime(df[c])
            existentes.append(c)
    if not existentes:
        raise ValueError("No se encontraron columnas de fecha válidas. Ajusta FECHAS_COLS.")
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

    # Último vendedor = el de la fila con la última fecha
    join_cols = [COL_CLIENTE, "FechaVenta_Fila", COL_VENDEDOR]
    join_cols = [c for c in join_cols if c in df.columns]
    df_join = df[join_cols].copy()

    dir_df = (
        agg.merge(
            df_join,
            left_on=[COL_CLIENTE, "UltimaFechaVenta"],
            right_on=[COL_CLIENTE, "FechaVenta_Fila"],
            how="left",
        )
        .sort_values([COL_CLIENTE, "FechaVenta_Fila"], ascending=[True, False])
        .drop_duplicates(subset=[COL_CLIENTE], keep="first")
        .rename(columns={COL_VENDEDOR: "UltimoVendedor"})
    )

    out = dir_df[
        [COL_CLIENTE, "Estatus", "Dias_desde_ultima_venta", "UltimoVendedor", "UltimaFechaVenta"]
    ].copy()

    return out.sort_values(["Estatus", "Dias_desde_ultima_venta"], ascending=[True, True]).reset_index(drop=True)

def to_excel_bytes(df: pd.DataFrame) -> bytes:
    bio = io.BytesIO()
    with pd.ExcelWriter(bio, engine="xlsxwriter", datetime_format="yyyy-mm-dd") as w:
        df.to_excel(w, index=False, sheet_name="Directorio")
        ws = w.sheets["Directorio"]
        ws.set_column(0, max(0, df.shape[1] - 1), 22)
    return bio.getvalue()

# --- Cargar catálogo externo opcional (si existiera archivo aparte) -----------
def cargar_catalogo_local(path: Path, sheet: str) -> pd.DataFrame | None:
    if not path.exists():
        return None
    cat = pd.read_excel(path, sheet_name=sheet)
    if CAT_LLAVE not in cat.columns:
        return None
    cat[CAT_LLAVE] = cat[CAT_LLAVE].astype("string").str.strip()
    cols = [c for c in [CAT_LLAVE, CAT_CIUDAD, CAT_EMAIL, CAT_PAIS, CAT_PROV, CAT_TEL] if c in cat.columns]
    return cat[cols].copy()

def cargar_catalogo_drive(file_id: str | None, sheet: str, json_path: str) -> pd.DataFrame | None:
    if not file_id:
        return None
    cat = read_excel_from_drive(file_id, sheet, json_path)
    if CAT_LLAVE not in cat.columns:
        return None
    cat[CAT_LLAVE] = cat[CAT_LLAVE].astype("string").str.strip()
    cols = [c for c in [CAT_LLAVE, CAT_CIUDAD, CAT_EMAIL, CAT_PAIS, CAT_PROV, CAT_TEL] if c in cat.columns]
    return cat[cols].copy()

# --- NUEVO: Cargar catálogo desde la hoja del MISMO Excel ---------------------
@st.cache_data(show_spinner=True)
def cargar_catalogo_mismo_excel(modo: str) -> pd.DataFrame | None:
    """
    Lee la hoja 'listado completo de clientes al' del mismo Excel de ventas.
    Interpreta columnas por letras (A..F) y renombra según CAT_MAP.
    """
    usecols = ",".join(CAT_MAP.keys())  # por ej. "C,A,B,D,E,F" (el orden lo conservamos)
    letras = list(CAT_MAP.keys())
    nombres = [CAT_MAP[l] for l in letras]

    try:
        if modo.lower() == "drive":
            cat = read_excel_from_drive(
                GDRIVE_FILE_ID_VENTAS,
                CATALOGO_EN_VENTAS_SHEET,
                GCP_SA_JSON_PATH,
                usecols=usecols,
                header=None,   # por si no hay encabezados formales
            )
        else:
            if not VENTAS_FILE.exists():
                return None
            cat = pd.read_excel(
                VENTAS_FILE,
                sheet_name=CATALOGO_EN_VENTAS_SHEET,
                usecols=usecols,
                header=None,
            )
        # Renombrar columnas en el mismo orden de "letras"
        cat.columns = nombres

        # Normaliza Cliente
        if CAT_LLAVE in cat.columns:
            cat[CAT_LLAVE] = cat[CAT_LLAVE].astype("string").str.strip()

        # Nos quedamos solo con columnas esperadas
        cols_ok = [c for c in [CAT_LLAVE, CAT_CIUDAD, CAT_EMAIL, CAT_PAIS, CAT_PROV, CAT_TEL] if c in cat.columns]

        # Si hay duplicados de Cliente, nos quedamos con el primero (o podrías agregar una lógica de preferencia)
        cat = cat.dropna(subset=[CAT_LLAVE]).drop_duplicates(subset=[CAT_LLAVE], keep="first")

        return cat[cols_ok].copy()
    except Exception as e:
        st.warning(f"No se pudo leer el catálogo de '{CATALOGO_EN_VENTAS_SHEET}': {e}")
        return None

def merge_catalogo(directorio: pd.DataFrame, cat: pd.DataFrame | None) -> pd.DataFrame:
    if cat is None:
        return directorio
    out = directorio.merge(cat, left_on=COL_CLIENTE, right_on=CAT_LLAVE, how="left")
    orden = [
        COL_CLIENTE,
        CAT_CIUDAD if CAT_CIUDAD in out.columns else None,
        CAT_EMAIL  if CAT_EMAIL  in out.columns else None,
        CAT_PAIS   if CAT_PAIS   in out.columns else None,
        CAT_PROV   if CAT_PROV   in out.columns else None,
        CAT_TEL    if CAT_TEL    in out.columns else None,
        "Estatus", "Dias_desde_ultima_venta", "UltimoVendedor", "UltimaFechaVenta",
    ]
    orden = [c for c in orden if c and c in out.columns]
    return out[orden].copy()


# === (E) UI ===================================================================
st.set_page_config(page_title="Directorio de clientes", layout="wide")
st.title("Directorio de clientes")

try:
    if MODO_CARGA.lower() == "drive":
        ventas_raw = read_excel_from_drive(GDRIVE_FILE_ID_VENTAS, SHEET_VENTAS, GCP_SA_JSON_PATH, dtype={COL_CLIENTE: "string"})
        ventas = preparar_ventas(ventas_raw)
        # 1) Catálogo desde la hoja del mismo Excel
        catalogo = cargar_catalogo_mismo_excel("drive")
        # 2) Si no hubo éxito, intento con archivo de catálogo separado en Drive (opcional)
        if catalogo is None:
            catalogo = cargar_catalogo_drive(GDRIVE_FILE_ID_CATALOGO, SHEET_CATALOGO, GCP_SA_JSON_PATH)
        st.caption(f"Fuente (Drive): fileId **{GDRIVE_FILE_ID_VENTAS}** · hoja ventas **{SHEET_VENTAS}** · catálogo **{CATALOGO_EN_VENTAS_SHEET}**")
    else:
        if not VENTAS_FILE.exists():
            raise FileNotFoundError(f"No encuentro {VENTAS_FILE.resolve()}")
        ventas_raw = pd.read_excel(VENTAS_FILE, sheet_name=SHEET_VENTAS, dtype={COL_CLIENTE: "string"})
        ventas = preparar_ventas(ventas_raw)
        # 1) Catálogo desde la hoja del mismo Excel
        catalogo = cargar_catalogo_mismo_excel("local")
        # 2) Si no, archivo de catálogo local opcional
        if catalogo is None:
            catalogo = cargar_catalogo_local(CATALOGO_FILE, SHEET_CATALOGO)
        st.caption(f"Fuente (local): **{VENTAS_FILE.name}** · hoja ventas **{SHEET_VENTAS}** · catálogo **{CATALOGO_EN_VENTAS_SHEET}**")

except Exception as e:
    st.error(f"Problema al leer los datos: {e}")
    st.stop()

base = calcular_directorio(ventas)
df = merge_catalogo(base, catalogo)

# Filtros
st.subheader("Buscar y filtrar")
fc1, fc2, fc3, fc4 = st.columns([2, 1, 1, 2])
texto = fc1.text_input("Buscar (Cliente / Ciudad / Correo / País / Provincia / Teléfono)", "")
estatus = fc2.selectbox("Estatus", ["Todos", "En uso", "Disponible"])
vendedores = ["Todos"] + sorted(df["UltimoVendedor"].dropna().astype(str).unique().tolist())
vendedor = fc3.selectbox("Vendedor", vendedores)
min_d = int(df["Dias_desde_ultima_venta"].min(skipna=True)) if "Dias_desde_ultima_venta" in df else 0
max_d = int(df["Dias_desde_ultima_venta"].max(skipna=True)) if "Dias_desde_ultima_venta" in df else 0
rango = fc4.slider(
    "Rango de días desde última venta",
    min_value=min(0, min_d),
    max_value=max(0, max_d),
    value=(min(0, min_d), max(0, max_d)),
)

# Aplicar filtros
f = df.copy()

def _contains_any(row, q: str) -> bool:
    q = q.lower()
    campos = [COL_CLIENTE, CAT_CIUDAD, CAT_EMAIL, CAT_PAIS, CAT_PROV, CAT_TEL]
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
cdesc1.download_button(
    "Descargar CSV",
    f.to_csv(index=False).encode("utf-8"),
    file_name="Directorio_clientes.csv",
    mime="text/csv",
)
cdesc2.download_button(
    "Descargar Excel",
    to_excel_bytes(f),
    file_name="Directorio_clientes.xlsx",
    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
)

with st.expander("Notas"):
    st.markdown(
        f"- **Estatus**: “En uso” si última venta < **{DIAS_EN_USO}** días; si no, “Disponible”.\n"
        "- **Último vendedor**: de la fila que coincide con **UltimaFechaVenta**.\n"
        "- Catálogo fusionado desde la hoja **{CATALOGO_EN_VENTAS_SHEET}** del mismo Excel.\n"
        f"- Modo de carga actual: **{MODO_CARGA.upper()}**."
    )

