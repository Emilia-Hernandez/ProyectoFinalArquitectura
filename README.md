# Proyecto Final: Spark Structured Streaming + Kafka + ML + Dashboard

Implementación end-to-end de un pipeline de datos en tiempo real para precios financieros usando `Spark Structured Streaming` y `Kafka` (Redpanda), con entrenamiento de un modelo de regresión y visualización web en `Streamlit`.

El streaming en tiempo real está configurado en modo **simulado** para estabilidad y replicabilidad.  
La API de Alpha Vantage se usa para descargar datos históricos diarios al entrenar (bootstrap del dataset).

## Qué incluye

1. Ingesta en tiempo real (`producer`) hacia Kafka.
2. Procesamiento con Spark Streaming (`stream_processor`) y cálculo de estadísticas por ventana.
3. Persistencia de datos `bronze`, `silver`, `stats` en Parquet.
4. Entrenamiento de regresión lineal (`train_model`) sobre datos recolectados.
5. Predicción en streaming (`stream_predictor`) sobre nueva tanda de datos.
6. Dashboard web (`Streamlit`) con métricas de streaming y desempeño del modelo.
7. Scripts para perfil de hardware y comparación de arquitecturas.

## Estructura de archivos

- `docker-compose.yml`: levanta Redpanda (Kafka-compatible) y su consola web.
- `requirements.txt`: dependencias Python para todo el pipeline.
- `.env.example`: variables de entorno del proyecto.
- `Makefile`: comandos cortos para correr cada componente.
- `app/config/settings.py`: configuración centralizada.
- `app/utils/logging_utils.py`: logger estándar del proyecto.
- `app/utils/schema.py`: esquema de eventos de mercado.
- `app/pipeline/producer.py`: productor Kafka en modo simulado continuo.
- `app/pipeline/stream_processor.py`: Spark Streaming para parseo, agregaciones y salida Parquet.
- `app/pipeline/train_model.py`: entrenamiento de regresión lineal sobre capa `silver`.
- `app/pipeline/stream_predictor.py`: inferencia online sobre micro-batches y guardado de predicciones.
- `app/data/bootstrap_data.py`: descarga histórico diario (free endpoint) o fallback sintético para entrenar siempre.
- `app/web/dashboard.py`: dashboard en Streamlit con Plotly.
- `scripts/hardware_profile.py`: genera perfil de hardware de la máquina.
- `scripts/benchmark_run.py`: guarda tiempos de ejecución básicos para comparación.
- `docs/architecture_comparison_template.md`: plantilla para comparar 2 arquitecturas según Spark UI.
- `tests/test_producer.py`: prueba mínima del simulador de datos.

## Requisitos

- Python 3.10+
- Docker + Docker Compose
- Java 11+ (para Spark)

## Configuración rápida

1. Crear entorno y dependencias:

```bash
make setup
```

2. Crear archivo de entorno:

```bash
cp .env.example .env
```

La clave incluida es:

```env
ALPHAVANTAGE_API_KEY=Y3YOW5NU1R7P9QIL
```

No es necesario configurar modo `live`; el productor es simulado por diseño.

Configuración de ventanas (dashboard y estadísticas):

- `STREAM_WINDOW_SECONDS=30`
- `STREAM_SLIDE_SECONDS=30`

Esto significa ventanas de 30 segundos tipo *tumbling* (sin traslape), por lo que verás una actualización nueva aproximadamente cada 30 segundos.

3. Levantar Kafka/Redpanda:

```bash
make kafka-up
```

- Consola Kafka: `http://localhost:8080`

## Ejecución del pipeline (4 terminales)

Terminal 1: productor

```bash
make producer-sim
```

(equivalente: `make producer`)

Terminal 2: procesamiento streaming con Spark

```bash
make stream
```

- Spark UI: `http://localhost:4040`

Terminal 3: entrenamiento (cuando ya haya datos en `output/silver`)

```bash
make train
```

Terminal 4: inferencia en streaming (segunda tanda)

```bash
make predict
```

Nota práctica para demo:
- Si `output/predictions` aún está vacío, el dashboard muestra una vista de predicción demo construida desde `output/silver`.
- Cuando `make predict` ya escribe batches, el dashboard cambia automáticamente a predicción online real.

Dashboard web (puede ir en otra terminal):

```bash
make dashboard
```

- Dashboard: `http://localhost:8501`

## Flujo esperado

1. `producer` publica eventos de mercado en Kafka topic `market_ticks`.
2. `stream_processor` consume el topic, parsea JSON y escribe:
   - `output/bronze`: datos crudos normalizados.
   - `output/silver`: features derivadas (`hl_spread`, `oc_change`, etc.).
   - `output/stats`: min, max, promedio, varianza y conteos por ventana.
3. `train_model` entrena regresión lineal y guarda modelo en `output/models/linear_regression.joblib`.
Si hay pocos datos en `silver`, completa automáticamente con histórico diario de Alpha Vantage; si la API no responde, usa histórico sintético local.
4. `stream_predictor` aplica modelo al stream y guarda predicciones en `output/predictions`.
5. `dashboard` visualiza estadísticos y calidad de predicción (MAE online).

## Replicabilidad en distintas arquitecturas

Para replicar en otra máquina:

1. Clonar repositorio.
2. Instalar Python, Java y Docker.
3. Ejecutar `make setup`.
4. Copiar `.env.example` a `.env`.
5. Correr pipeline con los mismos comandos del bloque anterior.

Para comparar arquitecturas (local vs cloud):

1. Ejecutar `python -m scripts.hardware_profile` en cada entorno.
2. Correr pipeline con igual configuración de `.env`.
3. Capturar métricas de Spark UI (jobs, stages, streaming rates, shuffle, GC, spill).
4. Completar `docs/architecture_comparison_template.md`.

## Pruebas

```bash
.venv/bin/pytest -q
```

## Limpieza

```bash
make clean
make kafka-down
```

## Notas

- El pipeline está diseñado para privilegiar la arquitectura correcta y reproducible; la precisión del modelo no es el foco principal.
- El endpoint intradía de Alpha Vantage puede requerir plan premium; por eso el streaming se deja simulado y se usa `TIME_SERIES_DAILY` para bootstrap de entrenamiento.

## Troubleshooting rápido

- Error `ModuleNotFoundError: No module named 'kafka.vendor.six.moves'`:
  - Causa: versión vieja de cliente Kafka incompatible con Python 3.12.
  - Solución:
    1. `rm -rf .venv`
    2. `make setup`
    3. `make producer-sim`
- Error `NoSuchMethodError ... scala.Predef$.wrapRefArray` en `make stream`:
  - Causa: incompatibilidad entre la versión/scala del conector Kafka y tu Spark local.
  - El proyecto ahora autodetecta `SPARK_HOME` para construir el paquete correcto, pero puedes forzarlo en `.env`.
  - Para tu entorno actual (Spark `3.5.4` compilado con `scala-2.13`), usa:
    1. `cp .env.example .env` (si aún no existe)
    2. agregar `SPARK_KAFKA_PACKAGE=org.apache.spark:spark-sql-kafka-0-10_2.13:3.5.4`
    3. volver a correr `make stream`
- Error `Not enough silver records to train model`:
  - Ya corregido en el pipeline actual.
  - `make train` ahora mezcla `silver` + bootstrap histórico para poder entrenar incluso con pocos datos iniciales.
- No se ven predicciones en el dashboard:
  - Verifica que `make predict` esté corriendo en una terminal aparte.
  - El predictor ahora reinicia su checkpoint automáticamente si no existen archivos en `output/predictions`.
  - Si aun así tarda en aparecer, el dashboard mostrará predicción demo desde `output/silver` para poder enseñar resultados.
