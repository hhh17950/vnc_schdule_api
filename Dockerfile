FROM python:3.11-slim AS builder
WORKDIR /build
COPY requirements.txt .
RUN python -m venv /opt/venv && /opt/venv/bin/pip install --no-cache-dir -r requirements.txt

FROM python:3.11-slim
ENV PATH="/opt/venv/bin:$PATH" PYTHONUNBUFFERED=1
WORKDIR /app
COPY --from=builder /opt/venv /opt/venv
COPY alembic.ini ./
COPY alembic ./alembic
COPY src ./src
RUN useradd --system --create-home vnc && mkdir /app/logs && chown vnc:vnc /app/logs
USER vnc
EXPOSE 8088
CMD ["uvicorn", "src.main:app", "--host", "0.0.0.0", "--port", "8088"]
