# Comparación de arquitecturas (plantilla)

Usa esta plantilla para comparar la misma app en dos entornos (ej. laptop local vs AWS/Colab).

## 1) Entornos

| Campo | Arquitectura A | Arquitectura B |
|---|---|---|
| Plataforma |  |  |
| CPU (núcleos) |  |  |
| RAM |  |  |
| GPU (si aplica) |  |  |
| Versión Python |  |  |
| Versión Spark |  |  |
| Kafka/Redpanda |  |  |

## 2) Métricas Spark UI

| Métrica | Arquitectura A | Arquitectura B | Comentario |
|---|---|---|---|
| Job execution time (s) |  |  |  |
| Shuffle read/write (MB) |  |  |  |
| I/O por stage |  |  |  |
| Scheduler Delay (ms) |  |  |  |
| Executor Run Time (ms) |  |  |  |
| GC Time (ms) |  |  |  |
| Spill Memory/Disk |  |  |  |
| Input rate (rows/s) |  |  |  |
| Processing rate (rows/s) |  |  |  |
| Batch duration (ms) |  |  |  |

## 3) Capturas sugeridas

1. Spark UI `Jobs`
2. Spark UI `Stages`
3. Spark UI `Structured Streaming`
4. Dashboard web (estadísticas + predicciones)
5. Consola Kafka/Redpanda (topic throughput)

## 4) Conclusiones

- ¿Qué arquitectura tuvo mejor latencia y throughput?
- ¿Dónde aparecieron cuellos de botella (shuffle, I/O, GC, spill)?
- ¿Qué cambios de configuración mejorarían la arquitectura más lenta?
