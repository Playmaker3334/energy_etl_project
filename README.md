# Proyecto ETL de Optimización Energética

Este proyecto implementa un pipeline ETL usando Apache Airflow y Docker para analizar datos sintéticos de consumo energético y generación solar.

## Estructura
- **Extract:** Generación de datos sintéticos (Python).
- **Transform:** Limpieza y cálculo de métricas de red (Pandas).
- **Load:** Almacenamiento en formato Parquet.
- **Visualization:** Dashboard interactivo en Streamlit.

## Cómo ejecutar

1. Asegúrate de tener Docker Desktop instalado.
2. Corre el comando:
   ```bash
   docker-compose up --build