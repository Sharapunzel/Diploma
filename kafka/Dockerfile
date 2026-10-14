FROM apache/kafka:4.2.0
USER root
RUN apk add --no-cache python3
COPY --chown=appuser:appuser bootstrap.py /opt/diploma/kafka/bootstrap.py
COPY --chown=appuser:appuser acls.py /opt/diploma/kafka/acls.py
USER appuser
ENTRYPOINT ["python3", "/opt/diploma/kafka/bootstrap.py"]
