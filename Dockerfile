FROM apache/airflow:3.3.1-python3.11

USER root
# Java 17 para Spark, independiente de la versión de Debian de la imagen
COPY --from=eclipse-temurin:17-jre /opt/java/openjdk /opt/java/openjdk
ENV JAVA_HOME=/opt/java/openjdk
ENV PATH="/opt/java/openjdk/bin:${PATH}"

# dbt en un venv aislado para no chocar con dependencias de Airflow
RUN python -m venv /opt/dbt-venv \
 && /opt/dbt-venv/bin/pip install --no-cache-dir dbt-core==1.12.3 dbt-postgres==1.11.0 \
 && chown -R airflow:0 /opt/dbt-venv

USER airflow
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir "apache-airflow==3.3.1" -r /requirements.txt
