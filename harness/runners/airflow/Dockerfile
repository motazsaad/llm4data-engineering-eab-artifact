FROM apache/airflow:3.3.1-python3.11

USER root
COPY entrypoint_test_dag.sh /entrypoint_test_dag.sh
RUN chmod +x /entrypoint_test_dag.sh && chown airflow:root /entrypoint_test_dag.sh

USER airflow
RUN pip install --no-cache-dir "requests>=2.28" "urllib3>=2.0" \
      "apache-airflow-providers-http==6.0.5"

ENTRYPOINT ["/entrypoint_test_dag.sh"]