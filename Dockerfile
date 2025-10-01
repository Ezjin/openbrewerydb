FROM apache/airflow:3.0.1-python3.10

# Instala pacotes adicionais necessários
RUN pip install --no-cache-dir \
    pandas \
    fastparquet \
    requests
