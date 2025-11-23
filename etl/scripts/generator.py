import pandas as pd
import numpy as np
import random
from datetime import datetime, timedelta
import os

def generate_energy_data(**kwargs):
    """
    Genera datos sintéticos de consumo de energía y producción solar.
    Guarda el resultado en la carpeta raw compartida.
    """
    # Configuración de rutas (Ruta interna del contenedor Docker)
    output_path = '/opt/airflow/data/raw'
    os.makedirs(output_path, exist_ok=True)
    
    # Generar rango de fechas (últimos 30 días, datos horarios)
    end_date = datetime.now()
    start_date = end_date - timedelta(days=30)
    date_range = pd.date_range(start=start_date, end=end_date, freq='H')
    n_rows = len(date_range)

    # 1. Simular Consumo de Red (Patrón diario: alto en día, bajo en noche)
    # Usamos función seno para simular el ciclo día/noche + ruido aleatorio
    hour_factor = np.sin(2 * np.pi * date_range.hour / 24)
    base_load = 50  # kWh mínimo
    variable_load = 30 * (hour_factor + 1) # Variación
    noise = np.random.normal(0, 5, n_rows) # Ruido aleatorio
    consumption = base_load + variable_load + noise

    # 2. Simular Generación Solar (0 de noche, curva de campana de día)
    # Horas de sol aprox entre 6am y 6pm
    solar_generation = []
    for hour in date_range.hour:
        if 6 <= hour <= 18:
            # Pico a las 12
            efficiency = 1 - (abs(12 - hour) / 6) 
            # Factor aleatorio de nubes
            cloud_factor = random.uniform(0.2, 1.0) 
            solar_generation.append(40 * efficiency * cloud_factor)
        else:
            solar_generation.append(0.0)

    # 3. Simular Temperatura (Correlacionada ligeramente con la hora)
    temperature = 20 + (10 * hour_factor) + np.random.normal(0, 2, n_rows)

    # Crear DataFrame
    df = pd.DataFrame({
        'timestamp': date_range,
        'consumption_kwh': consumption,
        'solar_generation_kwh': solar_generation,
        'temperature_c': temperature
    })

    # Introducir algunos valores nulos o anomalías para probar la limpieza (Fase Transform)
    # Ponemos un valor nulo arbitrario
    df.loc[random.randint(0, n_rows-1), 'temperature_c'] = np.nan
    
    # Guardar CSV
    file_name = 'energy_data_raw.csv'
    full_path = os.path.join(output_path, file_name)
    df.to_csv(full_path, index=False)
    
    print(f"Datos generados exitosamente en: {full_path}")
    return full_path