"""Punto de entrada: python scripts/run.py <comando>."""
import argparse
import importlib

# comando -> (modulo, funcion)
COMANDOS = {
    "referencias": ("focos.referencias", "cargar_departamentos"),
    "firms_disponibilidad": ("focos.extraccion_firms", "descargar_disponibilidad"),
    "firms_diario": ("focos.extraccion_firms", "descargar_diario"),
    "clima_diario": ("focos.extraccion_clima", "descargar_diario"),
    "bronze_focos": ("focos.bronze", "bronze_focos"),
    "bronze_clima": ("focos.bronze", "bronze_clima"),
    "silver_focos": ("focos.silver", "silver_focos"),
    "silver_clima": ("focos.silver", "silver_clima"),
    "exportar_postgres": ("focos.exportar_postgres", "exportar_postgres"),
    "firms_backfill": ("focos.extraccion_firms", "descargar_backfill"),
    "firms_sp_mensual": ("focos.extraccion_firms", "descargar_sp_mensual"),
    "clima_backfill": ("focos.extraccion_clima", "descargar_backfill"),
    "compactar_delta": ("focos.mantenimiento", "compactar_delta"),
    "borrar_snapshot_riesgo": ("focos.mantenimiento", "borrar_snapshot_riesgo"),
}


def main() -> None:
    parser = argparse.ArgumentParser(description="Comandos del pipeline de focos de calor y clima")
    parser.add_argument("comando", choices=sorted(COMANDOS))
    args = parser.parse_args()
    modulo, funcion = COMANDOS[args.comando]
    getattr(importlib.import_module(modulo), funcion)()


if __name__ == "__main__":
    main()
