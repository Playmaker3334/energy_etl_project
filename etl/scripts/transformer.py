import pandas as pd
import os

def transform_energy_data(**kwargs):
    """
    Lee el CSV raw, limpia datos, crea features y guarda en Parquet.
    """
    # Rutas internas del contenedor
    input_path = '/opt/airflow/data/raw/energy_data_raw.csv'
    output_path = '/opt/airflow/data/processed'
    os.makedirs(output_path, exist_ok=True)

    print(f"Iniciando transformación de: {input_path}")

    # Escalabilidad: Leer csv (pandas maneja bien este tamaño, para Big Data usaríamos chunks)
    try:
        df = pd.read_csv(input_path)
    except FileNotFoundError:
        raise Exception("El archivo raw no existe. Ejecuta la tarea de extracción primero.")

    # 1. Limpieza
    # Rellenar nulos en temperatura con el promedio (Imputación simple)
    if df['temperature_c'].isnull().sum() > 0:
        df['temperature_c'].fillna(df['temperature_c'].mean(), inplace=True)

    # Asegurar que no haya consumos negativos (Corrección de errores)
    df['consumption_kwh'] = df['consumption_kwh'].apply(lambda x: x if x > 0 else 0)

    # Conversión de tipos
    df['timestamp'] = pd.to_datetime(df['timestamp'])

    # 2. Feature Engineering (Creación de nuevas columnas)
    # Net Load: Cuánta energía realmente chupamos de la red vs solar
    df['net_grid_load'] = df['consumption_kwh'] - df['solar_generation_kwh']
    
    # Eficiencia: Si generamos más de lo que consumimos
    df['is_self_sufficient'] = df['net_grid_load'] <= 0

    # Costo Estimado (Ejemplo: $0.15 por kWh de red, Solar es "gratis")
    # Si net_load < 0, asumimos que vendemos a la red o se pierde (costo 0)
    df['estimated_cost'] = df['net_grid_load'].apply(lambda x: x * 0.15 if x > 0 else 0)

    # 3. Carga (Load) y Escalabilidad
    # Guardar en PARQUET (Formato columnar comprimido, mucho más rápido para leer en Dashboard)
    output_file = os.path.join(output_path, 'energy_data.parquet')
    df.to_parquet(output_file, index=False)

    print(f"Transformación completada. Archivo guardado en: {output_file}")
    print(f"Shape final: {df.shape}")