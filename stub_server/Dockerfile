FROM python:3.12-slim
WORKDIR /app
RUN pip install --no-cache-dir flask==3.0.0 gunicorn==21.2.0
COPY server.py .
EXPOSE 8080
CMD gunicorn -w ${STUB_WORKERS:-4} -b 0.0.0.0:8080 --timeout 30 --keep-alive 30 server:app
