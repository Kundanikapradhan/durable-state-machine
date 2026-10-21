# Production-Grade Container for Durable State Machine Workflows
# Security Best Practice: Minimal slim base, non-root user, persistent volume mount for WAL

FROM python:3.11-slim

# Prevent Python from writing .pyc files and enable unbuffered terminal logging
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    DATA_DIR=/app/data

# Create non-root system user for security compliance (Kubernetes best practice)
RUN groupadd -g 10001 appgroup && \
    useradd -u 10001 -g appgroup -m -s /bin/bash appuser

WORKDIR /app

# Install dependencies if present
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application source code
COPY --chown=appuser:appgroup core/ ./core/
COPY --chown=appuser:appgroup migration_workflow.py demo_crash_and_recover.py langgraph_workflow.py temporal_workflow.py ./
COPY --chown=appuser:appgroup tests/ ./tests/

# Create persistent checkpoint directory owned by non-root user
RUN mkdir -p /app/data && chown -R appuser:appgroup /app/data

# Switch to non-root user
USER appuser

# Container Healthcheck (verifies SQLite checkpointer is readable and responsive)
HEALTHCHECK --interval=30s --timeout=5s --start-period=5s --retries=3 \
    CMD python3 migration_workflow.py --db-path /app/data/checkpoints.db --status || exit 1

# Default command: Runs migration with persistent database in /app/data
CMD ["python3", "migration_workflow.py", "--db-path", "/app/data/checkpoints.db"]
