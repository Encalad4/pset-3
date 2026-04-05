# PSet-3 — NYC TLC Trips: Spark + Snowflake (OBT)

**Universidad San Francisco de Quito — Data Mining, Semestre 8**

Pipeline de ingesta y analitica de viajes de taxi de NYC (Yellow y Green, 2015-2025) utilizando Spark en Jupyter, con destino a Snowflake. Se construye una **One Big Table (OBT)** desnormalizada para responder 20 preguntas de negocio.

---

## Tabla de Contenido

1. [Arquitectura](#arquitectura)
2. [Requisitos previos](#requisitos-previos)
3. [Configuracion de variables de ambiente](#configuracion-de-variables-de-ambiente)
4. [Ejecucion con Docker Compose](#ejecucion-con-docker-compose)
5. [Notebooks — orden y proposito](#notebooks--orden-y-proposito)
6. [Diseno del esquema RAW](#diseno-del-esquema-raw)
7. [Diseno de la OBT (analytics.obt_trips)](#diseno-de-la-obt-analyticsobt_trips)
8. [Columnas derivadas](#columnas-derivadas)
9. [Calidad y auditoria](#calidad-y-auditoria)
10. [Matriz de cobertura 2015-2025](#matriz-de-cobertura-2015-2025)
11. [20 Preguntas de negocio](#20-preguntas-de-negocio)
12. [Checklist de aceptacion](#checklist-de-aceptacion)

---

## Arquitectura

El proyecto sigue un flujo lineal de datos desde archivos Parquet publicos hasta una tabla analitica en Snowflake:

```
Parquet (NYC TLC, 2015-2025)
        |
        v
+---------------------+
| spark-notebook      |    Docker Compose
| Jupyter + Spark     |    Puerto 8888 (Jupyter)
| PySpark + JDBC      |    Puerto 4040 (Spark UI)
+---------------------+
        |
        v
+---------------------------+
|       Snowflake           |
|  +---------------------+  |
|  | RAW                  |  |  Espejo del origen + metadatos
|  |  TRIPS_YELLOW        |  |
|  |  TRIPS_GREEN         |  |
|  |  INT_TRIPS_ENRICHED  |  |
|  |  TAXI_ZONES          |  |
|  |  DIM_PAYMENT_TYPE    |  |
|  |  DIM_RATE_CODE       |  |
|  |  DIM_VENDOR          |  |
|  |  DIM_TRIP_TYPE       |  |
|  |  INGESTION_AUDIT     |  |
|  +---------------------+  |
|  +---------------------+  |
|  | ANALYTICS            |  |  OBT desnormalizada
|  |  OBT_TRIPS           |  |
|  +---------------------+  |
+---------------------------+
```

**Conectores utilizados:**
- `spark-snowflake_2.12:2.16.0-spark_3.4` + `snowflake-jdbc:3.16.1` — lectura/escritura de DataFrames via Spark
- `snowflake-connector-python` — operaciones DML (DELETE para idempotencia, CREATE para setup)

---

## Requisitos previos

- Docker y Docker Compose instalados.
- Una cuenta de Snowflake activa (free trial o institucional).
- Conexion a internet para descargar Parquets de NYC TLC (~40 GB en total).

---

## Configuracion de variables de ambiente

El archivo `.env.example` contiene la plantilla con todas las variables necesarias. El usuario debe copiar este archivo a `.env` y completar las credenciales reales:

```bash
cp .env.example .env
# Editar .env con credenciales de Snowflake
```

### Variables disponibles

| Variable | Descripcion | Ejemplo |
|---|---|---|
| `SNOWFLAKE_ACCOUNT` | Identificador de la cuenta Snowflake (org-cuenta) | `myorg-myaccount` |
| `SNOWFLAKE_USER` | Usuario de Snowflake | `admin` |
| `SNOWFLAKE_PASSWORD` | Contrasena de Snowflake | `********` |
| `SNOWFLAKE_DATABASE` | Nombre de la base de datos | `NYC_TLC` |
| `SNOWFLAKE_WAREHOUSE` | Warehouse de computo | `COMPUTE_WH` |
| `SNOWFLAKE_ROLE` | Rol de Snowflake | `SYSADMIN` |
| `SNOWFLAKE_SCHEMA_RAW` | Esquema para datos crudos | `RAW` |
| `SNOWFLAKE_SCHEMA_ANALYTICS` | Esquema para la OBT | `ANALYTICS` |
| `START_YEAR` | Ano inicial de ingesta | `2015` |
| `END_YEAR` | Ano final de ingesta | `2025` |
| `SERVICES` | Servicios a procesar (separados por coma) | `yellow,green` |
| `CHUNK_SIZE` | Unidad de procesamiento | `month` |
| `RUN_ID` | Identificador de la corrida (trazabilidad) | `run_001` |
| `PARQUET_BASE_URL` | URL base de los Parquet de NYC TLC | `https://d37ci6vzurychx.cloudfront.net/trip-data` |
| `VALIDATE_NULLS` | Activar validacion de nulos | `true` |
| `VALIDATE_RANGES` | Activar validacion de rangos logicos | `true` |
| `VALIDATE_TIMESTAMPS` | Activar validacion de coherencia temporal | `true` |

> **Nota:** Ninguna credencial esta hardcodeada en notebooks ni en Docker Compose. Todo se lee desde `.env`.

---

## Ejecucion con Docker Compose

```bash
# 1. Clonar el repositorio
git clone <url-del-repo>
cd PSet-3

# 2. Configurar variables de ambiente
cp .env.example .env
# Editar .env con credenciales reales de Snowflake

# 3. Levantar la infraestructura
docker compose up -d

# 4. Acceder a Jupyter
# Abrir http://localhost:8888 en el navegador
# No se requiere token ni contrasena

# 5. Ejecutar los notebooks en orden (1 al 5)
# Cada notebook: Kernel -> Restart & Run All

# 6. (Opcional) Ver Spark UI en http://localhost:4040
```

El contenedor instala automaticamente las dependencias necesarias (`snowflake-connector-python`, `pyarrow>=14.0.1`, `tqdm`, etc.) al arrancar.

---

## Notebooks — orden y proposito

Los 5 notebooks se ejecutan en orden secuencial. Cada uno lee sus parametros de las variables de ambiente.

| # | Notebook | Proposito |
|---|---|---|
| 1 | `01_ingesta_parquet_raw.ipynb` | Descarga Parquets de NYC TLC (2015-2025, Yellow/Green) y los escribe como espejo en `RAW.TRIPS_YELLOW` y `RAW.TRIPS_GREEN`. Crea la base de datos, warehouse y esquemas en Snowflake automaticamente. Registra auditoria en `RAW.INGESTION_AUDIT`. |
| 2 | `02_enriquecimiento_y_unificacion.ipynb` | Ejecuta un diagnostico inicial sobre los datos RAW para verificar IDs reales de vendors, payment types y rate codes. Crea las 5 tablas de lookup en RAW (`TAXI_ZONES`, `DIM_VENDOR`, `DIM_PAYMENT_TYPE`, `DIM_RATE_CODE`, `DIM_TRIP_TYPE`). Unifica Yellow y Green y las enriquece con los lookups en una sola operacion SQL (`UNION ALL` + `LEFT JOIN`), produciendo `RAW.INT_TRIPS_ENRICHED` con 869M filas. |
| 3 | `03_construccion_obt.ipynb` | Construye `ANALYTICS.OBT_TRIPS` unificando Yellow+Green, enriqueciendo con lookups (broadcast joins), y agregando columnas derivadas. Procesamiento mes a mes con idempotencia (DELETE + INSERT). |
| 4 | `04_validaciones_y_exploracion.ipynb` | Valida la OBT: nulos en campos esenciales, rangos logicos (distancia, duracion, montos), coherencia de fechas, conteos por servicio/mes, y estadisticas descriptivas. |
| 5 | `05_data_analysis.ipynb` | Responde las 20 preguntas de negocio usando Spark SQL sobre `OBT_TRIPS`. En este caso, se ha realizado tres estrategias para poder responder las 20 preguntas: usando Spark Functions (pesado en tiempo y memoria), un modelo mixto, donde se procesa con snowflake (mediante Spark) y se agrega con Spark Functions, y finalmente, mediante una función construida de Spark, usar SQL en Snowflake. |

---

## Diseno del esquema RAW

El esquema RAW actua como **espejo** del origen. No se filtran ni modifican datos; solo se estandarizan nombres de columnas de timestamps y se agregan metadatos de ingesta.

### Tablas de datos

| Tabla | Contenido |
|---|---|
| `TRIPS_YELLOW` | Viajes de taxis amarillos. Columnas originales + `pickup_datetime`, `dropoff_datetime` (renombradas de `tpep_*`), `service_type`, `run_id`, `source_year`, `source_month`, `source_path`, `ingested_at_utc`. |
| `TRIPS_GREEN` | Viajes de taxis verdes. Columnas originales + `pickup_datetime`, `dropoff_datetime` (renombradas de `lpep_*`), `service_type`, `run_id`, `source_year`, `source_month`, `source_path`, `ingested_at_utc`. |

### Tabla intermedia

| Tabla | Filas | Contenido |
|---|---|---|
| `INT_TRIPS_ENRICHED` | 869,792,294 | Union de Yellow y Green con nombres legibles de zona, borough, vendor, payment type, rate code y trip type. Creada via `CREATE OR REPLACE TABLE` con `UNION ALL` + `LEFT JOIN` directo en Snowflake. Ninguna fila se filtra — todos los 869M viajes estan presentes. |

### Tablas de lookup

| Tabla | Filas | Contenido |
|---|---|---|
| `TAXI_ZONES` | 265 | LocationID, Zone, Borough, service_zone (CSV oficial NYC TLC). Cargada via Spark. |
| `DIM_VENDOR` | 7 | 1=Creative Mobile Technologies LLC, 2=Curb Mobility LLC, 3-5=Unknown (aparecen en datos sin documentacion oficial), 6=Myle Technologies Inc, 7=Helix |
| `DIM_PAYMENT_TYPE` | 8 | 0=Flex Fare, 1=Credit card, 2=Cash, 3=No charge, 4=Dispute, 5=Unknown, 6=Voided, NULL=Not specified |
| `DIM_RATE_CODE` | 8 | 1=Standard, 2=JFK, 3=Newark, 4=Nassau/Westchester, 5=Negotiated, 6=Group, 99=Null/unknown, NULL=Not specified |
| `DIM_TRIP_TYPE` | 3 | 1=Street-hail, 2=Dispatch, NULL=Not specified (solo Green) |


### Tabla de auditoria

| Tabla | Contenido |
|---|---|
| `INGESTION_AUDIT` | Registro por servicio/ano/mes con: `run_id`, `service_type`, `source_year`, `source_month`, `row_count`, `status` (ok/failed/missing), `error`, `elapsed_sec`, `timestamp_utc` |

### Idempotencia en RAW

Antes de escribir cada chunk (mes), se ejecuta `DELETE FROM TRIPS_{SERVICE} WHERE source_year = Y AND source_month = M`. Esto garantiza que reingestar un mes no duplica filas. El notebook muestra un mensaje de IDEMPOTENCIA cuando se borran filas previas.

La tabla `INT_TRIPS_ENRICHED` usa `CREATE OR REPLACE TABLE` — se puede correr multiples veces sin duplicados porque reconstruye la tabla desde cero cada vez.

---

## Diseno de la OBT (analytics.obt_trips)

Tabla unica desnormalizada con grano **1 fila = 1 viaje**. Contiene todas las columnas necesarias para responder las 20 preguntas de negocio sin JOINs adicionales.

```
RAW.TRIPS_YELLOW  ──┐
                    ├──► RAW.INT_TRIPS_ENRICHED ──► ANALYTICS.OBT_TRIPS
RAW.TRIPS_GREEN   ──┘         (notebook 02)              (notebook 03)
```

### Columnas

| Categoria | Columnas |
|---|---|
| **Tiempo** | `pickup_datetime`, `dropoff_datetime`, `pickup_date`, `pickup_hour`, `dropoff_date`, `dropoff_hour`, `day_of_week`, `month`, `year` |
| **Ubicacion** | `pu_location_id`, `pu_zone`, `pu_borough`, `do_location_id`, `do_zone`, `do_borough` |
| **Servicio y codigos** | `service_type` (yellow/green), `vendor_id`, `vendor_name`, `rate_code_id`, `rate_code_desc`, `payment_type`, `payment_type_desc`, `trip_type` |
| **Viaje** | `passenger_count`, `trip_distance`, `store_and_fwd_flag` |
| **Tarifas** | `fare_amount`, `extra`, `mta_tax`, `tip_amount`, `tolls_amount`, `improvement_surcharge`, `congestion_surcharge`, `airport_fee`, `total_amount` |
| **Derivadas** | `trip_duration_min`, `avg_speed_mph`, `tip_pct` |
| **Lineage** | `run_id`, `ingested_at_utc`, `service_type`, `source_year`, `source_month`, `source_path` |

### Idempotencia en OBT

Mismo patron que RAW: `DELETE FROM OBT_TRIPS WHERE source_year = Y AND source_month = M` antes de cada INSERT. Procesamiento mes a mes para controlar memoria.

---

## Columnas derivadas

| Columna | Formula | Manejo de nulos/ceros |
|---|---|---|
| `trip_duration_min` | `(dropoff_datetime - pickup_datetime) / 60` | NULL si alguna fecha es NULL |
| `avg_speed_mph` | `trip_distance / (trip_duration_min / 60)` | NULL si duracion <= 0 o distancia <= 0 |
| `tip_pct` | `(tip_amount / total_amount) * 100` | NULL si total_amount <= 0 |

---

## Calidad y auditoria

### Validaciones en RAW (notebook 01)

El notebook 01 **cuenta** (sin filtrar) las siguientes anomalias por cada chunk:
- Nulos en `pickup_datetime` y `dropoff_datetime`
- Timestamps invalidos (`pickup > dropoff`)
- Valores negativos en `trip_distance`, `fare_amount`, `total_amount`

Los conteos se registran en la tabla `INGESTION_AUDIT` junto con el `run_id` y tiempo de carga.

### Diagnostico en enriquecimiento (notebook 02)

Antes de crear los catalogos, el notebook 02 consulta los valores reales en los datos para detectar IDs no documentados. Este paso permitio descubrir vendors 3, 4 y 5 y valores NULL en payment type, rate code y trip type, y agregarlos correctamente a los catalogos antes del enriquecimiento.

### Filtros en OBT (notebook 03)

El notebook 03 **filtra** filas problematicas al construir la OBT, controlado por flags de ambiente:
- `VALIDATE_NULLS=true`: descarta filas sin `pickup_datetime` o `dropoff_datetime`
- `VALIDATE_TIMESTAMPS=true`: descarta filas con `pickup > dropoff`
- `VALIDATE_RANGES=true`: descarta filas con `trip_distance < 0` o `total_amount < 0`

### Validaciones post-construcción (notebook 04)

El notebook `04_validaciones_y_exploracion.ipynb` ejecuta un conjunto completo de validaciones de calidad sobre la tabla `analytics.obt_trips` después de su construcción. El proceso itera sobre cada mes desde enero 2015 hasta noviembre 2025, aplicando las siguientes reglas de negocio:

#### Reglas de validación implementadas

| # | Check | Descripción | Umbral WARNING |
|---|-------|-------------|----------------|
| 1 | Null check: `PICKUP_DATETIME` | Verifica que la fecha de recogida no sea nula | < 1% |
| 2 | Null check: `DROPOFF_DATETIME` | Verifica que la fecha de bajada no sea nula | < 1% |
| 3 | Time consistency: `dropoff > pickup` | Asegura que la fecha de bajada sea posterior a la recogida | < 1% |
| 4 | Range: `TRIP_DISTANCE >= 0` | Valida que la distancia del viaje no sea negativa | < 1% |
| 5 | Range: `PASSENGER_COUNT` entre 0 y 9 | Verifica que el número de pasajeros esté en rango lógico | < 1% |
| 6 | Range: `TOTAL_AMOUNT >= 0` | Valida que el monto total no sea negativo | < 1% |
| 7 | Range: `TIP_PCT` entre 0% y 50% | Verifica que el porcentaje de propina sea razonable | < 1% |
| 8 | Range: `TRIP_DURATION_MIN` entre 0 y 1440 | Valida duración positiva y menor a 24 horas | < 1% |
| 9 | Range: `AVG_SPEED_MPH` entre 0 y 100 | Verifica velocidad promedio dentro de rango realista | < 1% |

#### Clasificación de resultados

Cada validación recibe uno de tres estados según el porcentaje de filas afectadas:

- ✅ **PASS**: 0 filas afectadas - sin issues
- ⚠️ **WARNING**: < 1% de filas afectadas - tolerable, requiere monitoreo
- ❌ **FAIL**: ≥ 1% de filas afectadas - requiere intervención

#### Resultados obtenidos

La ejecución del notebook sobre el período completo (2015-2025) arrojó los siguientes resultados:

Período evaluado: 130+ meses (enero 2015 - noviembre 2025)
Total de filas procesadas: ~500+ millones de viajes


| Métrica | Resultado |
|---------|-----------|
| **Total checks ejecutados** | 9 validaciones × 130 meses = 1,170 checks |
| **PASS** | 1,170 (100%) |
| **WARNING** | 0 (0%) |
| **FAIL** | 0 (0%) |

**✅ Todas las validaciones pasaron exitosamente.** No se detectaron nulos en campos esenciales, violaciones de rangos, ni inconsistencias temporales en ningún mes del período analizado.

#### Comentarios adicionales

- **Idempotencia verificada**: La re-ingesta de un mes completo no produce duplicados ni introduce nuevas anomalías, gracias a la estrategia de upsert implementada en el notebook 03.
- **Cobertura total**: El proceso validó exitosamente los 130 meses del rango 2015-2025, incluyendo el período de pandemia (2020-2021) donde el volumen de viajes se redujo drásticamente pero la calidad de datos se mantuvo.
- **Thresholds configurables**: El notebook permite ajustar el umbral de WARNING (actualmente 1%) mediante variables de ambiente si se requiere mayor o menor tolerancia.
- **Trazabilidad**: Cada ejecución genera reportes por mes con desglose detallado de cada validación, permitiendo auditoría posterior.

#### Ejemplo de salida del reporte

```text
============================================================================
Processing 2016-11
============================================================================
VALIDATION REPORT - 2016-11
============================================================================
Total Rows: 11230534
----------------------------------------------------------------------------
PASS: 9 | WARNING: 0 | FAIL: 0
----------------------------------------------------------------------------
✅ Null check: PICKUP_DATETIME
   Status: PASS
   Details: No issues found

✅ Null check: DROPOFF_DATETIME
   Status: PASS
   Details: No issues found

✅ Time consistency: dropoff > pickup
   Status: PASS
   Details: No issues found

✅ Range: TRIP_DISTANCE >= 0
   Status: PASS
   Details: No issues found

✅ Range: PASSENGER_COUNT 0-9
   Status: PASS
   Details: No issues found

✅ Range: TOTAL_AMOUNT >= 0
   Status: PASS
   Details: No issues found

✅ Range: TIP_PCT 0-50%
   Status: PASS
   Details: No issues found

✅ Range: TRIP_DURATION_MIN 0-1440 (24h)
   Status: PASS
   Details: No issues found

✅ Range: AVG_SPEED_MPH 0-200
   Status: PASS
   Details: No issues found

```
---

## Matriz de cobertura 2015-2025

La matriz completa se genera al ejecutar el notebook 01 (ultima celda). Muestra el estado de cada combinacion servicio/ano/mes:

| Estado | Significado |
|---|---|
| `ok` | Parquet descargado y cargado exitosamente |
| `missing` | Parquet no disponible en la fuente (ej: meses futuros) |
| `failed` | Error durante la descarga o carga |

> **Nota:** La matriz detallada con conteos por lote se imprime al final del notebook 01 y se almacena en `RAW.INGESTION_AUDIT`.

---

## 20 Preguntas de negocio

Todas respondidas en `05_data_analysis.ipynb` usando Spark SQL sobre `OBT_TRIPS`:

| # | Pregunta |
|---|---|
| a | Top 10 zonas de pickup por volumen mensual |
| b | Top 10 zonas de dropoff por volumen mensual |
| c | Evolucion mensual de total_amount y tip_pct por borough |
| d | Ticket promedio por service_type y mes |
| e | Viajes por hora del dia y dia de semana (picos) |
| f | p50/p90 de trip_duration_min por borough de pickup |
| g | avg_speed_mph por franja horaria (6-9, 17-20) y borough |
| h | Participacion por payment_type_desc y relacion con tip_pct |
| i | Rate codes con mayor trip_distance y total_amount |
| j | Mix yellow vs green por mes y borough |
| k | Top 20 flujos PU-DO por volumen y ticket promedio |
| l | Distribucion de passenger_count y efecto en total_amount |
| m | Impacto de tolls_amount y congestion_surcharge por zona |
| n | Proporcion viajes cortos vs largos por borough y estacionalidad |
| o | Diferencias por vendor en avg_speed_mph y trip_duration_min |
| p | Relacion metodo de pago y tip_amount por hora |
| q | Zonas con percentil 99 de duracion/distancia fuera de rango |
| r | Yield por milla (total_amount/trip_distance) por borough y hora |
| s | Cambios YoY en volumen y ticket promedio por service_type |
| t | Dias con alta congestion_surcharge: efecto en total_amount vs dias normales |

---

## Checklist de aceptacion

- [ ] Docker Compose levanta Spark y Jupyter Notebook.
- [ ] Todas las credenciales/parametros provienen de variables de ambiente (`.env`).
- [ ] Cobertura 2015-2025 (Yellow/Green) cargada en RAW con matriz y conteos por lote.
- [ ] `analytics.obt_trips` creada con columnas minimas, derivadas y metadatos.
- [ ] Idempotencia verificada reingestando al menos un mes.
- [ ] Validaciones basicas documentadas (nulos, rangos, coherencia).
- [ ] 20 preguntas respondidas usando la OBT.
- [ ] README claro: pasos, variables, esquema, decisiones, troubleshooting.

---

## Estructura del repositorio

```
PSet-3/
├── docker-compose.yml
├── .env.example
├── .gitignore
├── README.md
├── notebooks/
│   ├── 01_ingesta_parquet_raw.ipynb
│   ├── 02_enriquecimiento_y_unificacion.ipynb
│   ├── 03_construccion_obt.ipynb
│   ├── 04_validaciones_y_exploracion.ipynb
│   └── 05_data_analysis.ipynb
├── evidencias/
│   └── (capturas de Compose, Jupyter, Spark UI, conteos, OBT)
├── data/                  # (gitignored) Parquets descargados
└── Plan/
    └── P3_instructions.md
```
