# Pipeline automatic trigger (S3 watcher)

Este repositorio contiene un pipeline que procesa archivos Parquet ubicados en la zona `raw/` de un bucket S3. Para automatizar la ejecución al momento de insertar un archivo nuevo en `raw/`, se incluye un watcher por sondeo (`s3_watcher.py`) que detecta archivos nuevos y dispara el procesamiento.

Archivos importantes:
- `pipeline.py`: lógica de limpieza y generación de zonas `cleaned/` y `curated/`.
- `s3_watcher.py`: servicio liviano que consulta S3 periódicamente y llama a `pipeline.process_file` para cada archivo nuevo.
- `processed_files.json` (almacenado en S3 en `metadata/processed_files.json`): marcador de archivos ya procesados.

Variables de entorno esperadas (.env):

- BUCKET_NAME: nombre del bucket S3
- ACCESS_KEY: AWS access key
- SECRET_KEY: AWS secret key
- POLL_INTERVAL_SECONDS (opcional): intervalo de sondeo en segundos (default 60)

Uso local (macOS / zsh):

1. Crear un archivo `.env` con las variables anteriores.
2. Instalar dependencias (ej. boto3, pyspark, python-dotenv).
3. Ejecutar el watcher:

```bash
python s3_watcher.py
```

Despliegue como servicio:
- macOS: crear un LaunchAgent que ejecute `python /path/to/s3_watcher.py`.
- Linux: crear una unidad systemd que ejecute el script.
- Alternativa: contenerizar la app y desplegar en ECS, Kubernetes o similar.

Notas y buenas prácticas:
- El watcher usa sondeo por simplicidad. Para producción, recomiendo configurar eventos S3 (SNS/SQS/Lambda) para una latencia y costos menores.
- `pipeline.py` ya marca archivos procesados en `metadata/processed_files.json` para evitar reprocesos y es idempotente por archivo.
- Vigilar el uso de recursos: `pipeline.py` inicia un SparkSession; si esperas procesar muchos archivos concurrentes, considera reusar la misma sesión o adaptar la arquitectura a jobs por lotes.

Siguientes pasos recomendados:
- Implementar pruebas unitarias y de integración.
- Añadir métricas y logs estructurados (CloudWatch, Prometheus).
- Soporte para reintentos exponenciales y DLQ en caso de fallos persistentes.
