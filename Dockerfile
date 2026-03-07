# Usamos una imagen ligera de Python
FROM python:3.11-slim

# Evita que Python genere archivos .pyc y permite ver logs en tiempo real
ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONUNBUFFERED 1

# Directorio de trabajo dentro del contenedor
WORKDIR /app

# Instalamos dependencias del sistema para psycopg2 (base de datos)
RUN apt-get update && apt-get install -y \
    build-essential \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# Copiamos e instalamos las librerías de Python
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copiamos el resto del código
COPY . .

# Exponemos el puerto que usa Streamlit
EXPOSE 8501

# Comando para arrancar la app
CMD ["streamlit", "run", "src/app.py", "--server.port=8501", "--server.address=0.0.0.0"]
