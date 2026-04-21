# Archivos que debes pasarle a ChatGPT para hacer el reporte

Este proyecto implementa un pipeline end-to-end de datos en tiempo real con Kafka/Redpanda, Spark Structured Streaming, entrenamiento de ML e interfaz web en Streamlit. Para que ChatGPT pueda redactar un buen reporte, no necesita recibir todo el proyecto ni carpetas generadas. Debe recibir los archivos que explican arquitectura, ejecucion, configuracion, procesamiento, modelo, dashboard y medicion.

## Paquete minimo recomendado

Pasa estos archivos en este orden:

1. `README.md`
   - Es el archivo principal de contexto.
   - Explica el objetivo del proyecto, flujo esperado, comandos, carpetas de salida y orden correcto de ejecucion.

2. `Instrucciones.docx`
   - Contiene los requisitos originales del trabajo.
   - Es importante para que el reporte conecte la implementacion con lo que pidio el curso.

3. `docker-compose.yml`
   - Explica la capa de infraestructura.
   - Muestra que Kafka se implementa con Redpanda y que tambien se levanta la consola web.

4. `Makefile`
   - Resume como se ejecuta cada parte del sistema.
   - Sirve para documentar comandos de instalacion, arranque, limpieza, entrenamiento, prediccion, dashboard y benchmark.

5. `requirements.txt`
   - Lista las tecnologias usadas: PySpark, Kafka, pandas, scikit-learn, Streamlit, Plotly, pyarrow, etc.
   - Sirve para la seccion de entorno y dependencias.

6. `.env.example`
   - Explica las variables de configuracion del sistema: Kafka, topic, ventanas, Spark, carpetas de salida y tasa del productor.
   - No pases `.env` si contiene llaves reales o datos privados. Usa solo `.env.example`.

## Codigo principal que tambien debes pasar

Estos archivos explican la implementacion real. Son los mas importantes despues del README:

1. `app/config/settings.py`
   - Centraliza configuracion del proyecto.
   - Define API key, simbolo financiero, Kafka, Spark, ventanas, tasa del productor y rutas de salida.
   - Tambien detecta version de Spark/Scala para resolver el paquete Kafka de Spark.

2. `app/utils/schema.py`
   - Define el esquema de los eventos de mercado.
   - Campos principales: `event_time`, `symbol`, `open`, `high`, `low`, `close`, `volume`, `source`.

3. `app/pipeline/producer.py`
   - Implementa el productor Kafka.
   - Genera datos simulados de mercado a una tasa configurable, por defecto `4096` eventos por segundo.
   - Publica los eventos en el topic `market_ticks`.

4. `app/pipeline/stream_processor.py`
   - Es el nucleo de Spark Structured Streaming.
   - Consume eventos desde Kafka.
   - Parsea JSON con el esquema del proyecto.
   - Escribe capas `bronze`, `silver` y estadisticas por ventana.
   - Calcula min, max, promedio, varianza, volumen promedio y conteos por ventana.
   - Guarda metricas de progreso del streaming para comparacion de arquitecturas.

5. `app/data/bootstrap_data.py`
   - Prepara datos para entrenamiento.
   - Lee datos de `output/silver`.
   - Si hay pocos datos, intenta usar Alpha Vantage.
   - Si no hay API o falla la peticion, genera historico sintetico para que el entrenamiento sea replicable.

6. `app/pipeline/train_model.py`
   - Entrena un modelo de regresion lineal.
   - Usa features como `open`, `high`, `low`, `close`, `volume` y `hl_spread`.
   - Predice `next_close`.
   - Guarda el artefacto en `output/models/linear_regression.joblib`.
   - Reporta MAE y RMSE.

7. `app/pipeline/stream_predictor.py`
   - Carga el modelo entrenado.
   - Consume nuevos eventos desde Kafka.
   - Aplica inferencia por micro-batches.
   - Guarda predicciones en `output/predictions`.

8. `app/web/dashboard.py`
   - Implementa el dashboard en Streamlit.
   - Visualiza estadisticas por ventana, predicciones, error absoluto, MAE online, perfil de hardware y benchmark.
   - Es clave para la seccion de resultados y visualizacion.

## Archivos para la parte de comparacion de arquitecturas

Pasa estos si tu reporte debe incluir comparacion local vs cloud, otra laptop, Colab, AWS, etc.

1. `scripts/hardware_profile.py`
   - Genera un perfil de la maquina: plataforma, CPU, nucleos, version de Python y RAM.
   - Guarda el resultado en `output/logs/hardware_profile.json`.

2. `scripts/benchmark_run.py`
   - Ejecuta comandos medidos y guarda tiempos en `output/logs/benchmark_runs.csv`.
   - Sirve para comparar tiempos entre arquitecturas.

3. `docs/architecture_comparison_template.md`
   - Plantilla para capturar metricas de Spark UI.
   - Incluye input rate, processing rate, batch duration, shuffle, GC, spill, scheduler delay, executor run time, etc.

## Archivos de pruebas

No son centrales para explicar la arquitectura, pero conviene pasarlos si el reporte debe mencionar validacion o pruebas.

1. `tests/test_producer.py`
   - Prueba que el simulador genere los campos esperados.
   - Confirma que el productor crea eventos con simbolo y fuente simulada.

2. `tests/conftest.py`
   - Configura el path para que las pruebas importen el paquete `app`.

## Archivos que NO debes pasar

No pases estos archivos o carpetas al chat del reporte:

1. `.env`
   - Puede contener secretos o configuracion local.
   - Usa `.env.example` en su lugar.

2. `.venv/`
   - Es el entorno virtual. Es muy grande y no aporta al reporte.

3. `output/`
   - Contiene artefactos generados: parquet, checkpoints, modelo, predicciones y logs.
   - Solo pasa archivos especificos de `output/logs` si ya generaste resultados y quieres que el reporte incluya numeros reales.

4. `__pycache__/` y archivos `*.pyc`
   - Son cache de Python. No sirven para documentar el proyecto.

5. `.pytest_cache/`
   - Cache de pytest. No aporta al reporte.

6. `.DS_Store`
   - Archivo interno de macOS.

7. `~$strucciones.docx`
   - Archivo temporal de Word. No es el documento real.

8. `.git/`
   - Historial interno de Git. No es necesario para redactar el reporte.

## Si quieres pasar solo lo indispensable

Si el chat tiene limite de archivos, pasa solamente estos:

```text
README.md
Instrucciones.docx
docker-compose.yml
Makefile
requirements.txt
.env.example
app/config/settings.py
app/utils/schema.py
app/pipeline/producer.py
app/pipeline/stream_processor.py
app/data/bootstrap_data.py
app/pipeline/train_model.py
app/pipeline/stream_predictor.py
app/web/dashboard.py
docs/architecture_comparison_template.md
```

Con eso ya puede redactarse un reporte completo con:

- objetivo del proyecto,
- arquitectura general,
- tecnologias utilizadas,
- flujo de datos,
- configuracion de Kafka y Spark,
- procesamiento por ventanas,
- modelo de machine learning,
- inferencia en streaming,
- dashboard,
- replicabilidad,
- metricas para comparacion de arquitecturas,
- instrucciones de ejecucion,
- conclusiones tecnicas.

## Prompt sugerido para pedir el reporte

Puedes pegar este texto junto con los archivos:

```text
Necesito redactar un reporte academico sobre este proyecto. El sistema implementa un pipeline de datos en tiempo real con Kafka/Redpanda, Spark Structured Streaming, entrenamiento de un modelo de regresion y dashboard en Streamlit.

Usa los archivos adjuntos para escribir un reporte claro y tecnico que incluya:
1. Introduccion y objetivo del proyecto.
2. Requisitos originales del trabajo y como se cumplen.
3. Arquitectura general del sistema.
4. Descripcion de cada componente: productor, Kafka/Redpanda, Spark Streaming, capas bronze/silver/stats, entrenamiento, prediccion y dashboard.
5. Flujo de datos paso a paso.
6. Tecnologias y dependencias usadas.
7. Configuracion principal: topic, tasa de eventos, ventanas, rutas de salida y Spark.
8. Explicacion del modelo de regresion lineal, features, target y metricas MAE/RMSE.
9. Explicacion del dashboard y metricas visualizadas.
10. Estrategia de replicabilidad y comparacion entre arquitecturas.
11. Pruebas o validacion.
12. Limitaciones del proyecto y posibles mejoras.
13. Conclusion.

Redactalo en espanol, con tono formal academico, pero que se entienda. Incluye tablas si ayudan.
```

## Nota sobre resultados reales

Si ya corriste el pipeline y quieres que el reporte incluya resultados medidos, genera y pasa tambien:

```text
output/logs/hardware_profile.json
output/logs/benchmark_runs.csv
output/logs/stream_progress/stats_snapshot.json
```

Esos archivos no siempre existen. Se generan al correr:

```bash
python -m scripts.hardware_profile
make benchmark
make stream
```

Tambien puedes incluir capturas de:

- Dashboard Streamlit: `http://localhost:8501`
- Spark UI: `http://localhost:4040`
- Redpanda Console: `http://localhost:8080`

