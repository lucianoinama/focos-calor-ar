"""Focos de calor de NASA FIRMS (VIIRS) -> data/landing/firms."""
import csv
import time
from datetime import date, timedelta

from focos.config import (
    LANDING_DIR, conectar_dw, hoy_utc, http_get, log, ocultar_secretos, tabla_existe, variable,
)

URL_BASE = "https://firms.modaps.eosdis.nasa.gov/api"
DIR_FIRMS = LANDING_DIR / "firms"
SOURCES_NRT = {
    "VIIRS_SNPP_NRT": "SNPP",
    "VIIRS_NOAA20_NRT": "NOAA20",
    "VIIRS_NOAA21_NRT": "NOAA21",
}
COLUMNAS_OBLIGATORIAS = {"latitude", "longitude", "acq_date", "acq_time", "confidence", "frp", "daynight"}
MAX_DIAS_POR_PEDIDO = 5
PAUSA_SEG = 1


def _columnas(encabezado: str) -> set[str]:
    return {columna.strip() for columna in encabezado.split(",")}


def descargar_disponibilidad() -> None:
    """Comando `firms_disponibilidad`: fechas disponibles por sensor."""
    url = f"{URL_BASE}/data_availability/csv/{variable('FIRMS_MAP_KEY')}/ALL"
    texto = http_get(url).text
    encabezado = texto.splitlines()[0] if texto else ""
    if not {"data_id", "min_date", "max_date"} <= _columnas(encabezado):
        raise RuntimeError(f"Formato inesperado de disponibilidad: {ocultar_secretos(encabezado[:300])}")
    DIR_FIRMS.mkdir(parents=True, exist_ok=True)
    (DIR_FIRMS / "disponibilidad.csv").write_text(texto, encoding="utf-8")
    log.info("Disponibilidad guardada: %s filas", len(texto.splitlines()) - 1)


def _validar_csv(texto: str, source: str) -> int:
    """Verifica el encabezado y devuelve la cantidad de focos."""
    lineas = texto.splitlines()
    if not lineas:
        raise RuntimeError(f"{source}: respuesta vacia")
    faltan = COLUMNAS_OBLIGATORIAS - _columnas(lineas[0])
    if faltan:
        raise RuntimeError(
            f"{source}: faltan columnas {sorted(faltan)}. Encabezado: {ocultar_secretos(lineas[0][:300])}"
        )
    return len(lineas) - 1


def _ultima_fecha_cargada(satelite: str) -> date | None:
    """Ultima fecha_utc del satelite en raw.focos, o None si la tabla no existe."""
    conexion = conectar_dw()
    try:
        if not tabla_existe(conexion, "raw", "focos"):
            return None
        with conexion.cursor() as cur:
            cur.execute("select max(fecha_utc) from raw.focos where satelite = %s", (satelite,))
            return cur.fetchone()[0]
    finally:
        conexion.close()


def _rango(satelite: str) -> tuple[date, date]:
    """Rango de fechas UTC a descargar para el satelite."""
    hoy = hoy_utc()
    ultima = _ultima_fecha_cargada(satelite)
    if ultima is None:
        desde = hoy - timedelta(days=int(variable("FIRMS_DIAS_INICIALES")) - 1)
    else:
        desde = min(hoy - timedelta(days=1), ultima + timedelta(days=1))
    return desde, hoy


def descargar_diario() -> None:
    """Comando `firms_diario`: focos NRT recientes de los 3 satelites."""
    clave, bbox = variable("FIRMS_MAP_KEY"), variable("FIRMS_BBOX")
    for source, satelite in SOURCES_NRT.items():
        desde, hasta = _rango(satelite)
        destino = DIR_FIRMS / source
        destino.mkdir(parents=True, exist_ok=True)
        inicio = desde
        while inicio <= hasta:
            dias = min(MAX_DIAS_POR_PEDIDO, (hasta - inicio).days + 1)
            url = f"{URL_BASE}/area/csv/{clave}/{source}/{bbox}/{dias}/{inicio.isoformat()}"
            texto = http_get(url).text
            focos = _validar_csv(texto, source)
            archivo = destino / f"diario_{inicio.isoformat()}_{dias}d.csv"
            archivo.write_text(texto, encoding="utf-8")
            log.info("%s: %s focos en %s", source, focos, archivo.name)
            inicio += timedelta(days=dias)
            time.sleep(PAUSA_SEG)

# --- Etapa 6: historico y productos SP ---
SOURCES_HISTORICO = {
    # satelite: (source SP o None, source NRT)
    "SNPP": ("VIIRS_SNPP_SP", "VIIRS_SNPP_NRT"),
    "NOAA20": ("VIIRS_NOAA20_SP", "VIIRS_NOAA20_NRT"),
    "NOAA21": (None, "VIIRS_NOAA21_NRT"),
}


def _leer_disponibilidad() -> dict[str, tuple[date, date]]:
    """data_id -> (min_date, max_date) desde disponibilidad.csv."""
    with (DIR_FIRMS / "disponibilidad.csv").open(encoding="utf-8", newline="") as archivo:
        return {
            fila["data_id"].strip(): (
                date.fromisoformat(fila["min_date"].strip()),
                date.fromisoformat(fila["max_date"].strip()),
            )
            for fila in csv.DictReader(archivo)
        }


def _rango_disponible(disponibilidad: dict, source: str) -> tuple[date, date]:
    if source not in disponibilidad:
        raise RuntimeError(f"{source} no figura en disponibilidad.csv. data_id recibidos: {sorted(disponibilidad)}")
    return disponibilidad[source]


def _descargar_tramos(source: str, desde: date, hasta: date) -> set[date]:
    """Baja [desde, hasta] en tramos de hasta 5 dias; saltea archivos existentes. Devuelve los dias cubiertos."""
    clave, bbox = variable("FIRMS_MAP_KEY"), variable("FIRMS_BBOX")
    destino = DIR_FIRMS / source
    destino.mkdir(parents=True, exist_ok=True)
    cubiertos: set[date] = set()
    inicio = desde
    while inicio <= hasta:
        dias = min(MAX_DIAS_POR_PEDIDO, (hasta - inicio).days + 1)
        archivo = destino / f"{inicio.isoformat()}_{dias}d.csv"
        if not archivo.exists():
            url = f"{URL_BASE}/area/csv/{clave}/{source}/{bbox}/{dias}/{inicio.isoformat()}"
            texto = http_get(url).text
            focos = _validar_csv(texto, source)
            archivo.write_text(texto, encoding="utf-8")
            log.info("%s: %s focos en %s", source, focos, archivo.name)
            time.sleep(PAUSA_SEG)
        cubiertos.update(inicio + timedelta(days=i) for i in range(dias))
        inicio += timedelta(days=dias)
    return cubiertos


def descargar_backfill() -> None:
    """Comando `firms_backfill`: historico SP + NRT de BACKFILL_FECHA_INICIO a BACKFILL_FECHA_FIN."""
    descargar_disponibilidad()
    disponibilidad = _leer_disponibilidad()
    inicio = date.fromisoformat(variable("BACKFILL_FECHA_INICIO"))
    fin = date.fromisoformat(variable("BACKFILL_FECHA_FIN"))
    huecos = []
    for satelite, (source_sp, source_nrt) in SOURCES_HISTORICO.items():
        cubiertos: set[date] = set()
        sp_max = None
        if source_sp:
            sp_min, sp_max = _rango_disponible(disponibilidad, source_sp)
            desde, hasta = max(inicio, sp_min), min(fin, sp_max)
            if desde <= hasta:
                cubiertos |= _descargar_tramos(source_sp, desde, hasta)
            else:
                log.info("%s: sin SP en el rango", satelite)
        nrt_min, nrt_max = _rango_disponible(disponibilidad, source_nrt)
        desde = max(inicio, nrt_min) if sp_max is None else max(inicio, nrt_min, sp_max + timedelta(days=1))
        hasta = min(fin, nrt_max)
        if desde <= hasta:
            cubiertos |= _descargar_tramos(source_nrt, desde, hasta)
        else:
            log.info("%s: sin NRT en el rango", satelite)
        dia = inicio
        while dia <= fin:
            if dia not in cubiertos:
                huecos.append((satelite, dia.isoformat()))
            dia += timedelta(days=1)
    with (DIR_FIRMS / "huecos_backfill.csv").open("w", encoding="utf-8", newline="") as archivo:
        escritor = csv.writer(archivo, lineterminator="\n")
        escritor.writerow(["satelite", "fecha"])
        escritor.writerows(huecos)
    log.info("Backfill FIRMS terminado. Dias sin cobertura: %s", len(huecos))


def _ultima_fecha_sp(satelite: str) -> date | None:
    conexion = conectar_dw()
    try:
        if not tabla_existe(conexion, "raw", "focos"):
            return None
        with conexion.cursor() as cur:
            cur.execute(
                "select max(fecha_utc) from raw.focos where satelite = %s and producto = 'SP'", (satelite,)
            )
            return cur.fetchone()[0]
    finally:
        conexion.close()


def descargar_sp_mensual() -> None:
    """Comando `firms_sp_mensual`: SP nuevos desde el ultimo cargado."""
    descargar_disponibilidad()
    disponibilidad = _leer_disponibilidad()
    for satelite, (source_sp, _) in SOURCES_HISTORICO.items():
        if source_sp is None:
            continue
        _, sp_max = _rango_disponible(disponibilidad, source_sp)
        ultima = _ultima_fecha_sp(satelite)
        desde = ultima + timedelta(days=1) if ultima else date.fromisoformat(variable("BACKFILL_FECHA_INICIO"))
        if desde > sp_max:
            log.info("%s: no hay SP nuevos (ultimo cargado %s, disponible hasta %s)", satelite, ultima, sp_max)
            continue
        _descargar_tramos(source_sp, desde, sp_max)
