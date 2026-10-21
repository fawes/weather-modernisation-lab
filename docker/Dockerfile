# Production Container for FastAPI OGC API - EDR & Weather Lakehouse Service
# Replaces legacy BP RHEL Apache Tomcat VMs with a stateless Linux container
FROM python:3.12-slim-bookworm

WORKDIR /app

# Install native C-libraries for scientific weather processing (NetCDF / ecCodes)
RUN apt-get update && apt-get install -y --no-install-recommends \
    libnetcdf-dev libeccodes-dev curl && \
    rm -rf /var/lib/apt/lists/*

# Install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application source and STAC/Lakehouse metadata
COPY src/ ./src/
COPY data/ ./data/

# Run as non-root user for enterprise container security (EKS / AKS / GKE / Cloud Run)
RUN useradd -m weatherapp && chown -R weatherapp:weatherapp /app
USER weatherapp

EXPOSE 8000

# Container-native Health Check used by AWS ALB / Azure App Gateway / GCP Load Balancer
HEALTHCHECK --interval=15s --timeout=5s --start-period=10s --retries=3 \
    CMD curl -f http://localhost:8000/ || exit 1

CMD ["uvicorn", "src.09_day4_stac_and_ogc_api:app", "--host", "0.0.0.0", "--port", "8000", "--workers", "2"]
