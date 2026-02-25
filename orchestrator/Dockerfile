FROM python:3.12-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY benchmark.py report.py ./
ENTRYPOINT ["python", "-u"]
CMD ["benchmark.py", "--help"]
