FROM python:3.11-slim

WORKDIR /app
COPY recorder_sink.py /app/recorder_sink.py
EXPOSE 8055

CMD ["python3", "/app/recorder_sink.py", "--port", "8055", "--fixtures-dir", "/fixtures", "--log", "/out/sink.jsonl", "--trace", "/out/trace.jsonl"]