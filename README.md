# Focos de calor y clima en Argentina

Pipeline de datos de punta a punta que cruza los focos de calor detectados por satélite (NASA FIRMS) con el clima diario de los 529 departamentos de Argentina, desde enero de 2026. Está construido con Apache Airflow, PySpark con Delta Lake, PostgreSQL con PostGIS y dbt, todo en Docker sobre una PC común.

> **English summary.** End-to-end data pipeline that joins NASA FIRMS satellite fire detections with daily weather for Argentina's 529 departments since January 2026. Medallion architecture (Bronze and Silver in Delta Lake with PySpark, Gold in PostgreSQL/PostGIS with dbt), orchestrated with Apache Airflow and fully containerized with Docker. Features spatial joins, idempotent MERGE loads, SCD Type 2 history, 34 data tests and a Power BI dashboard.

## Qué resuelve

¿Qué condiciones climáticas acompañan a los focos de calor en Argentina? El proyecto permite ver dónde y cuándo se concentran los focos, cómo se relacionan con las rachas sin lluvia, la humedad y el viento (la regla 30-30-30), y cómo evoluciona mes a mes el riesgo de cada departamento.

## Arquitectura

```
NASA FIRMS ─┐
Open-Meteo ─┼─ Airflow ─► Landing ─► Bronze ─► Silver ─► PostgreSQL + PostGIS ─► Power BI
Georef/IGN ─┘            (crudo)    (Delta,   (Delta,    (dbt: modelo estrella,
                                    texto)    tipado,     cruce espacial,
                                              MERGE)      SCD2 y tests)
```

| Capa | Dónde | Qué tiene |
| --- | --- | --- |
| Landing | `data/landing/` | Archivos tal cual llegan de las APIs (CSV y JSON) |
| Bronze | Delta Lake | Todo como texto, con metadatos de origen. Solo agrega filas |
| Silver | Delta Lake | Datos tipados y deduplicados, consolidados con `MERGE` |
| Gold | PostgreSQL + PostGIS | Modelo estrella construido con dbt |

## Stack

| Herramienta | Versión | Para qué |
| --- | --- | --- |
| Apache Airflow | 3.3.1 | Orquestación (LocalExecutor) |
| PySpark + Delta Lake | 3.5.9 + 3.3.3 | Capas Bronze y Silver |
| PostgreSQL + PostGIS | 16 + 3.5 | Data warehouse y cruce espacial |
| dbt-core + dbt-postgres | 1.12 + 1.11 | Transformaciones, tests y snapshots |
| Docker Compose | — | Todo el entorno en contenedores |
| Python | 3.11 | Extracción y jobs |
| Power BI | — | Tablero |

## Datos

| Fuente | Qué aporta | Volumen |
| --- | --- | --- |
| [NASA FIRMS](https://firms.modaps.eosdis.nasa.gov/) (VIIRS: Suomi NPP, NOAA-20 y NOAA-21) | Focos de calor detectados por satélite | 460.259 focos descargados; 247.616 dentro de Argentina |
| [Open-Meteo](https://open-meteo.com/) (API histórica) | Temperatura máxima, humedad mínima, lluvia y viento máximo diarios | 148.649 filas (529 departamentos × día), del 2026-01-01 al 2026-10-08 |
| [Georef / IGN](https://www.argentina.gob.ar/georef) | Polígonos de los departamentos | 529 departamentos de 24 jurisdicciones |

## Modelo de datos

| Tabla (`marts`) | Grano | Contenido |
| --- | --- | --- |
| `dim_fecha` | Día | Calendario con mes, día de la semana y estación del año |
| `dim_departamento` | Departamento | Nombre, provincia, superficie y punto de referencia |
| `dim_sensor` | Satélite | Plataforma, instrumento y resolución |
| `fct_focos` | Foco de calor | Ubicación, intensidad (FRP), confianza, día o noche y riesgo vigente del departamento |
| `fct_clima_diario` | Departamento × día | Clima, días sin lluvia, regla 30-30-30 y focos del día |

El snapshot `snapshots.snap_riesgo_departamento` guarda la historia de la categoría de riesgo mensual de cada departamento (SCD tipo 2, 1.715 versiones).

**Reglas de negocio:**

- **Día seco:** menos de 1 mm de lluvia. Las rachas se calculan con el método *gaps and islands*.
- **Regla 30-30-30:** más de 30 °C, menos de 30 % de humedad y viento de más de 30 km/h.
- **Riesgo mensual:** `alto` si hubo 50 focos o más o 5 días 30-30-30 o más; `medio` si hubo 10 focos o más o al menos un día 30-30-30; si no, `bajo`. Rige desde el primer día del mes siguiente.

## Pipelines

| DAG | Qué hace | Cuándo |
| --- | --- | --- |
| `pipeline_diario` | Extracción, Bronze, Silver, exportación, `dbt run`, `dbt test` y frescura | Cada día (`scripts/actualizar.sh`) |
| `mensual_sp_y_riesgo` | Focos definitivos (SP), snapshot del riesgo y compactación de Delta | El día 2 de cada mes |
| `backfill_focos`, `backfill_clima`, `backfill_riesgo` | Carga del histórico y reconstrucción del SCD2 | Una vez |
| `setup_referencias` | Departamentos a PostGIS | Una vez |

## Decisiones técnicas

- **Idempotencia en todas las capas:** Bronze registra qué archivos ya procesó, Silver avanza con una marca de agua y consolida con `MERGE`. Correr el pipeline dos veces no duplica filas.
- **Datos provisorios y definitivos:** FIRMS publica primero focos casi en tiempo real (NRT) y meses después la versión definitiva (SP). Silver reemplaza los días que ya tienen SP con `replaceWhere`.
- **Cruce espacial en PostGIS:** cada foco se asigna a su departamento con `ST_Contains` sobre un índice GIST; los focos fuera de Argentina se descartan.
- **SCD tipo 2 con dbt:** snapshot con estrategia `check` y `updated_at` igual a la fecha de vigencia, cargado mes por mes en orden cronológico.
- **Respeto de cuotas:** el histórico de clima usa un presupuesto de llamadas por corrida y es reanudable, para no superar el límite gratuito de Open-Meteo.
- **Pensado para una PC con pocos recursos:** Spark en modo local con 2 GB de driver, tareas en serie, un solo servidor Postgres para Airflow y el data warehouse, y compactación mensual de Delta (`OPTIMIZE` y `VACUUM`).
- **dbt aislado:** dbt corre en su propio entorno virtual dentro de la imagen, para no chocar con las dependencias de Airflow.

## Calidad de datos

- 34 tests de dbt: unicidad, valores aceptados, relaciones del modelo estrella, rangos válidos, completitud del clima por departamento y día, y una sola versión vigente por departamento en el SCD2.
- Chequeo de frescura de las fuentes en cada corrida diaria.
- Cada etapa del proyecto se cerró con una verificación escrita: conteos entre capas, pruebas de idempotencia y pruebas negativas (datos malos a propósito para confirmar que los tests los detectan).

## Cómo levantarlo

**Requisitos:** Docker Desktop (en Windows, con WSL2), unos 6 GB de RAM para Docker, 2 GB libres de disco y una [MAP_KEY gratuita de FIRMS](https://firms.modaps.eosdis.nasa.gov/api/map_key/).

```bash
git clone <url-de-este-repo> focos-calor-ar && cd focos-calor-ar
cp .env.example .env
# Completar en .env los valores COMPLETAR y AIRFLOW_UID (salida de: id -u).
# FERNET_KEY:  docker run --rm apache/airflow:3.3.1-python3.11 python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
# JWT secret:  openssl rand -hex 32
docker compose build
docker compose up airflow-init
docker compose up -d
```

Carga inicial (Airflow queda en `http://localhost:8080`):

```bash
scripts/correr_dag.sh setup_referencias 30
scripts/correr_dag.sh backfill_focos 240
scripts/correr_dag.sh backfill_clima 240   # repetir al día siguiente si quedan pendientes (cuota de Open-Meteo)
scripts/actualizar.sh
scripts/correr_dag.sh backfill_riesgo 60
```

Uso habitual: `scripts/actualizar.sh` para actualizar los datos y, el día 2 de cada mes, `scripts/correr_dag.sh mensual_sp_y_riesgo 180`.

## Tablero de Power BI

Capturas en `docs/img/` (próximamente).

## Limitaciones

- **Un foco de calor no siempre es un incendio forestal:** también detecta quemas agrícolas y fuentes industriales.
- **El clima se toma en un punto por departamento** y viene de un modelo de reanálisis, no de estaciones meteorológicas; en departamentos grandes es una aproximación.
- **Las rachas sin lluvia se cuentan desde el 2026-01-01**, así que las de los primeros días pueden estar subestimadas.
- **Los focos de los últimos meses son provisorios (NRT)** hasta que FIRMS publica la versión definitiva.
- **Días sin cobertura de FIRMS en el período:** 0.
- **La categoría de riesgo es una regla propia** para el análisis, no un índice oficial de peligro de incendio.

## Fuentes y licencias

- Focos de calor: NASA FIRMS (Fire Information for Resource Management System), parte del sistema ESDIS de NASA.
- Clima: [Open-Meteo.com](https://open-meteo.com/), bajo licencia CC BY 4.0.
- Departamentos: API Georef de Argentina, con datos del IGN, bajo licencia CC BY 4.0.
- Código: licencia MIT (ver `LICENSE`).

## Autor

**Luciano Agustín Inama**: [LinkedIn](https://www.linkedin.com/in/agustin-inama-76a785152/)
