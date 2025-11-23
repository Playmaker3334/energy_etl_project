import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import os
from datetime import datetime, timedelta

# --- CONFIGURACIÓN GENERAL ---
st.set_page_config(
    page_title="Energy Analytics Platform",
    layout="wide",
    initial_sidebar_state="expanded"
)

# --- ESTILOS CSS AVANZADOS (UI PROFESIONAL) ---
st.markdown("""
    <style>
        /* Quitar el padding superior excesivo */
        .block-container {
            padding-top: 2rem;
            padding-bottom: 2rem;
        }
        
        /* Estilo para las Tarjetas de Métricas (KPIs) */
        div[data-testid="metric-container"] {
            background-color: #1a1c24; /* Fondo oscuro tarjeta */
            border-radius: 8px;
            padding: 20px;
            box-shadow: 0 4px 6px rgba(0, 0, 0, 0.3);
            transition: transform 0.2s;
        }
        
        div[data-testid="metric-container"]:hover {
            transform: translateY(-2px);
        }

        /* Texto de las métricas */
        div[data-testid="metric-container"] > label {
            font-size: 14px;
            color: #a0a0a0;
            font-weight: 500;
            text-transform: uppercase;
            letter-spacing: 1px;
        }
        
        div[data-testid="metric-container"] > div[data-testid="stMetricValue"] {
            font-size: 28px;
            font-weight: 700;
            color: #ffffff;
        }

        /* BORDES DE COLOR PARA INDICADORES (Reemplazan a los iconos) */
        /* El truco es usar selectores nth-child. 
           Nota: Esto asume el orden de las columnas en el código */
        
        /* KPI 1: Consumo (Rojo) */
        div[data-testid="column"]:nth-of-type(1) div[data-testid="metric-container"] {
            border-left: 4px solid #FF4560;
        }
        
        /* KPI 2: Solar (Dorado/Amarillo) */
        div[data-testid="column"]:nth-of-type(2) div[data-testid="metric-container"] {
            border-left: 4px solid #FEB019;
        }
        
        /* KPI 3: Eficiencia (Verde/Azul) */
        div[data-testid="column"]:nth-of-type(3) div[data-testid="metric-container"] {
            border-left: 4px solid #00E396;
        }
        
        /* KPI 4: CO2 (Verde Hoja) */
        div[data-testid="column"]:nth-of-type(4) div[data-testid="metric-container"] {
            border-left: 4px solid #775DD0;
        }

    </style>
""", unsafe_allow_html=True)

# --- CARGA DE DATOS ---
DATA_PATH = '/app/data/processed/energy_data.parquet'

@st.cache_data
def load_data():
    if not os.path.exists(DATA_PATH):
        return None
    df = pd.read_parquet(DATA_PATH)
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    return df

df = load_data()

# --- SIDEBAR ---
with st.sidebar:
    st.markdown("### CONFIGURACIÓN")
    
    if df is not None:
        min_date = df['timestamp'].min().date()
        max_date = df['timestamp'].max().date()
        
        start_date, end_date = st.date_input(
            "Rango de Fechas",
            value=(max_date - timedelta(days=7), max_date),
            min_value=min_date,
            max_value=max_date
        )
    else:
        st.warning("Data not found.")
        start_date, end_date = None, None
    
    st.markdown("---")
    st.markdown("""
        <div style='font-size: 12px; color: #666;'>
        BUILD VERSION: 2.0.1<br>
        PIPELINE STATUS: ACTIVE
        </div>
    """, unsafe_allow_html=True)

# --- DASHBOARD PRINCIPAL ---
if df is not None and start_date and end_date:
    mask = (df['timestamp'].dt.date >= start_date) & (df['timestamp'].dt.date <= end_date)
    df_filtered = df.loc[mask]

    # Título limpio
    st.markdown("## Energy Optimization Monitor")
    st.markdown(f"<div style='color: #666; margin-bottom: 20px;'>Analysis period: {start_date} to {end_date}</div>", unsafe_allow_html=True)

    # --- KPIS ---
    col1, col2, col3, col4 = st.columns(4)
    
    # Cálculos
    total_consumption = df_filtered['consumption_kwh'].sum()
    total_solar = df_filtered['solar_generation_kwh'].sum()
    
    # Evitar división por cero
    if total_consumption > 0:
        solar_coverage = (total_solar / total_consumption) * 100
    else:
        solar_coverage = 0
        
    co2_saved = total_solar * 0.4

    # Métricas (Sin emojis, el CSS pone los bordes de color)
    col1.metric("Grid Consumption", f"{total_consumption:,.0f} kWh")
    col2.metric("Solar Generation", f"{total_solar:,.0f} kWh", delta=f"{total_solar/total_consumption:.1%} mix")
    col3.metric("Self Sufficiency", f"{solar_coverage:.1f} %")
    col4.metric("Carbon Offset", f"{co2_saved:,.0f} kg")

    st.markdown("<br>", unsafe_allow_html=True)

    # --- GRÁFICO 1: ÁREA (CLEAN DESIGN) ---
    st.markdown("##### Power Flow Dynamics")
    
    fig_main = go.Figure()

    # Generación Solar (Area Rellena)
    fig_main.add_trace(go.Scatter(
        x=df_filtered['timestamp'], 
        y=df_filtered['solar_generation_kwh'],
        mode='lines',
        name='Solar',
        stackgroup='one', 
        line=dict(width=0),
        fillcolor='rgba(254, 176, 25, 0.5)' # Naranja/Dorado suave
    ))

    # Consumo Red (Línea)
    fig_main.add_trace(go.Scatter(
        x=df_filtered['timestamp'], 
        y=df_filtered['consumption_kwh'],
        mode='lines',
        name='Grid Load',
        line=dict(color='#FF4560', width=2.5) # Rojo moderno
    ))

    # Estilo minimalista del gráfico
    fig_main.update_layout(
        paper_bgcolor='rgba(0,0,0,0)',
        plot_bgcolor='rgba(0,0,0,0)',
        hovermode="x unified",
        height=400,
        xaxis=dict(showgrid=False, color='#888'),
        yaxis=dict(showgrid=True, gridcolor='#333', color='#888', title="kWh"),
        legend=dict(orientation="h", y=1.05, x=1, xanchor="right", font=dict(color='#ccc')),
        margin=dict(l=0, r=0, t=20, b=0)
    )
    st.plotly_chart(fig_main, use_container_width=True)

    # --- GRÁFICOS INFERIORES ---
    c_left, c_right = st.columns([2, 1])

    with c_left:
        st.markdown("##### Hourly Efficiency Profile")
        
        # Agrupar por hora
        df_filtered['hour'] = df_filtered['timestamp'].dt.hour
        hourly_avg = df_filtered.groupby('hour')[['consumption_kwh', 'solar_generation_kwh']].mean().reset_index()

        fig_bar = go.Figure()
        fig_bar.add_trace(go.Bar(
            x=hourly_avg['hour'], 
            y=hourly_avg['consumption_kwh'], 
            name='Consumption',
            marker_color='#2b2d3e' # Grid color (Gris azulado oscuro)
        ))
        fig_bar.add_trace(go.Bar(
            x=hourly_avg['hour'], 
            y=hourly_avg['solar_generation_kwh'], 
            name='Solar',
            marker_color='#FEB019' # Amarillo Solar
        ))

        fig_bar.update_layout(
            barmode='overlay',
            paper_bgcolor='rgba(0,0,0,0)',
            plot_bgcolor='rgba(0,0,0,0)',
            height=300,
            xaxis=dict(title="Hour of Day", showgrid=False, color='#888'),
            yaxis=dict(showgrid=True, gridcolor='#333', color='#888'),
            legend=dict(orientation="h", y=1.1, font=dict(color='#ccc')),
            margin=dict(l=0, r=0, t=30, b=0)
        )
        st.plotly_chart(fig_bar, use_container_width=True)

    with c_right:
        st.markdown("##### Energy Source Mix")
        
        # Calcular déficit real
        net_grid = (df_filtered['consumption_kwh'] - df_filtered['solar_generation_kwh']).clip(lower=0).sum()
        
        # Donut chart minimalista
        fig_donut = go.Figure(data=[go.Pie(
            labels=['Grid Import', 'Solar Gen'], 
            values=[net_grid, total_solar], 
            hole=.7,
            marker=dict(colors=['#2b2d3e', '#FEB019']),
            textinfo='percent',
            hoverinfo='label+value'
        )])
        
        fig_donut.update_layout(
            paper_bgcolor='rgba(0,0,0,0)',
            plot_bgcolor='rgba(0,0,0,0)',
            height=300,
            showlegend=True,
            legend=dict(orientation="h", y=0, font=dict(color='#ccc')),
            margin=dict(l=0, r=0, t=30, b=0)
        )
        st.plotly_chart(fig_donut, use_container_width=True)

else:
    st.info("Waiting for ETL pipeline execution...")