FROM apache/airflow:3.0.1

# Instala pacotes adicionais necessários
RUN pip install --no-cache-dir \
    pandas \
    pyarrow \
    requests
