# Usar el entorno de Debian para trabajar
FROM debian:latest

# Crear el directorio gdelt_app
WORKDIR /gdelt_app

# Agregar el archivo test.py que está en este directorio y pasarlo al directorio de esta imágen
COPY test.py /

# Entrar al directorio
RUN cd /gdelt_app

# Actualizar paquetes
RUN apt-get update

# Instalar Python y Python venv
RUN apt-get install python3 python3-venv -y

# Crear un entorno virtual
RUN python3 -m venv .venv

# Iniciar el entorno
RUN . .venv/bin/activate

# Instalar gdelt para el entorno
RUN .venv/bin/python .venv/bin/pip install gdelt

# Instalar geopandas para el entorno
RUN .venv/bin/python .venv/bin/pip install geopandas

# Instalar pyspark para el entorno
RUN .venv/bin/python .venv/bin/pip install pyspark

# Instalar pyarrow para el entorno
RUN .venv/bin/python .venv/bin/pip install pyarrow

EXPOSE 5000

