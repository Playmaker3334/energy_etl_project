# Usamos Python ligero
FROM python:3.9-slim

# Directorio de trabajo
WORKDIR /app

# Copiar requerimientos e instalar
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copiar el código de la app
COPY app.py .

# Exponer puerto
EXPOSE 8501

# Ejecutar Streamlit
CMD ["streamlit", "run", "app.py", "--server.address=0.0.0.0"]