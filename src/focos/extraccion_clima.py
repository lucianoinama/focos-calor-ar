"""Clima diario por departamento (Open-Meteo, API historica) -> data/landing/openmeteo."""
import csv
import json
import time
from datetime import date, datetime, timedelta, timezone

from focos.config import (
    LANDING_DIR, REFERENCE_DIR, conectar_dw, hoy_ar, http_get, log, tabla_existe, variable,
)

URL = "https://archive-api.open-meteo.com/v1/archive"
VARIABLES = ["temperature_2m_max", "relative_humidity_2m_min", "precipitation_sum", "wind_speed_10m_max"]
ZONA_HORARIA = "America/Argentina/Buenos_Aires"
ARCHIVO_PUNTOS = REFERENCE_DIR / "departamentos_puntos.csv"
DIR_DIARIO = LANDING_DIR / "openmeteo" / "diario"


def _leer_puntos() -> list[dict]:
    """Puntos internos de los departamentos, generados por el comando referencias."""
    if not ARCHIVO_PUNTOS.exists():
        raise RuntimeError(f"No existe {ARCHIVO_PUNTOS}: correr primero el comando referencias")
    with ARCHIVO_PUNTOS.open(encoding="utf-8") as archivo:
        return list(csv.DictReader(archivo))


def _ultima_fecha_cargada() -> date | None:
    """Ultima fecha en raw.clima_diario, o None si la tabla no existe."""
    conexion = conectar_dw()
    try:
        if not tabla_existe(conexion, "raw", "clima_diario"):
            return None
        with conexion.cursor() as cur:
            cur.execute("select max(fecha) from raw.clima_diario")
            return cur.fetchone()[0]
    finally:
        conexion.close()


def _rango() -> tuple[date, date]:
    """Rango de fechas (Argentina) a descargar."""
    hoy = hoy_ar()
    fin = hoy - timedelta(days=1)
    inicio = hoy - timedelta(days=int(variable("OPENMETEO_DIAS_INICIALES")))
    ultima = _ultima_fecha_cargada()
    if ultima is not None:
        inicio = min(inicio, ultima + timedelta(days=1))
    return inicio, fin


def _validar(datos: dict, departamento_id: str, dias_esperados: int) -> None:
    """Verifica que la respuesta tenga las variables y la cantidad de dias pedidas."""
    diario = datos.get("daily")
    if not diario or "time" not in diario:
        raise RuntimeError(f"{departamento_id}: falta 'daily' en la respuesta. Claves: {sorted(datos)}")
    faltan = [nombre for nombre in VARIABLES if nombre not in diario]
    if faltan:
        raise RuntimeError(f"{departamento_id}: faltan variables {faltan}")
    if len(diario["time"]) != dias_esperados:
        raise RuntimeError(f"{departamento_id}: {len(diario['time'])} dias, se esperaban {dias_esperados}")


def descargar_diario() -> None:
    """Comando `clima_diario`: un JSON por departamento con el clima del rango."""
    puntos = _leer_puntos()
    inicio, fin = _rango()
    dias = (fin - inicio).days + 1
    pausa = float(variable("OPENMETEO_PAUSA_DIARIO_SEG"))
    destino = DIR_DIARIO / hoy_ar().isoformat()
    destino.mkdir(parents=True, exist_ok=True)
    peso_total = 0.0
    for numero, punto in enumerate(puntos, start=1):
        parametros = {
            "latitude": punto["punto_lat"],
            "longitude": punto["punto_lon"],
            "start_date": inicio.isoformat(),
            "end_date": fin.isoformat(),
            "daily": ",".join(VARIABLES),
            "timezone": ZONA_HORARIA,
        }
        datos = http_get(URL, params=parametros).json()
        _validar(datos, punto["departamento_id"], dias)
        (destino / f"{punto['departamento_id']}.json").write_text(json.dumps(datos), encoding="utf-8")
        peso_total += max(1.0, dias / 14)
        if numero % 100 == 0:
            log.info("Clima: %s de %s departamentos", numero, len(puntos))
        time.sleep(pausa)
    log.info(
        "Clima %s a %s: %s departamentos, peso aproximado %.1f llamadas",
        inicio, fin, len(puntos), peso_total,
    )

# --- Etapa 6: historico con presupuesto de llamadas ---
DIR_BACKFILL = LANDING_DIR / "openmeteo" / "backfill"
ARCHIVO_ESTADO = LANDING_DIR / "openmeteo" / "backfill_estado.json"


def descargar_backfill() -> None:
    """Comando `clima_backfill`: un JSON por departamento con todo el rango historico."""
    puntos = _leer_puntos()
    inicio = date.fromisoformat(variable("BACKFILL_FECHA_INICIO"))
    fin = date.fromisoformat(variable("BACKFILL_FECHA_FIN"))
    dias = (fin - inicio).days + 1
    peso_pedido = max(1.0, dias / 14) * max(1.0, len(VARIABLES) / 10)
    presupuesto = float(variable("OPENMETEO_PRESUPUESTO_POR_CORRIDA"))
    pausa = float(variable("OPENMETEO_PAUSA_BACKFILL_SEG"))
    DIR_BACKFILL.mkdir(parents=True, exist_ok=True)

    peso, motivo = 0.0, "completo"
    for punto in puntos:
        archivo = DIR_BACKFILL / f"{punto['departamento_id']}.json"
        if archivo.exists():
            continue
        if peso + peso_pedido > presupuesto:
            motivo = "presupuesto"
            break
        parametros = {
            "latitude": punto["punto_lat"],
            "longitude": punto["punto_lon"],
            "start_date": inicio.isoformat(),
            "end_date": fin.isoformat(),
            "daily": ",".join(VARIABLES),
            "timezone": ZONA_HORARIA,
        }
        try:
            datos = http_get(URL, params=parametros).json()
        except RuntimeError as exc:
            if "429" in str(exc):
                motivo = "limite_429"
                log.warning("Open-Meteo devolvio 429 tras los reintentos; se corta la corrida")
                break
            raise
        _validar(datos, punto["departamento_id"], dias)
        archivo.write_text(json.dumps(datos), encoding="utf-8")
        peso += peso_pedido
        time.sleep(pausa)

    completos = sum(1 for p in puntos if (DIR_BACKFILL / f"{p['departamento_id']}.json").exists())
    estado = {
        "total": len(puntos),
        "completos": completos,
        "pendientes": len(puntos) - completos,
        "peso_consumido": round(peso, 1),
        "fecha_inicio": inicio.isoformat(),
        "fecha_fin": fin.isoformat(),
        "motivo_fin": motivo,
        "actualizado_en": datetime.now(timezone.utc).isoformat(timespec="seconds"),
    }
    ARCHIVO_ESTADO.write_text(json.dumps(estado, indent=2), encoding="utf-8")
    log.info("Backfill clima: %s", estado)
