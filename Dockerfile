FROM apache/airflow:2.7.2
USER airflow
# Install additional Python packages, including MLFlow
RUN pip install mlflow pandas numpy seaborn matplotlib

