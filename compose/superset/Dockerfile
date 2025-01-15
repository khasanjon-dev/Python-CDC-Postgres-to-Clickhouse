FROM apache/superset:3.1.0

USER root
RUN mkdir -p /app/pythonpath

# Copy the entrypoint script
COPY compose/superset/entrypoint.sh /app/entrypoint.sh
RUN chmod +x /app/entrypoint.sh

COPY requirements/local.txt /app/pythonpath/requirements/local.txt
RUN pip install -r /app/pythonpath/requirements/local.txt

USER superset
ENTRYPOINT ["/app/entrypoint.sh"]
