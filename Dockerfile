FROM python:3.14-slim-bookworm

# Runtime libs: fonts for matplotlib, OpenMP for numpy/scipy
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        fonts-dejavu-core \
        libgomp1 && \
    rm -rf /var/lib/apt/lists/*

# Core dependencies (flute framework)
RUN pip install --no-cache-dir psutil redis pydantic

# Copy worker directories and install their dependencies
COPY workers/ /workers/
COPY scripts/install_worker_deps.py /tmp/install_worker_deps.py
RUN python /tmp/install_worker_deps.py && rm /tmp/install_worker_deps.py

# Non-root user with HOME=/tmp (writable via tmpfs)
RUN groupadd -r flute && useradd -r -g flute -d /tmp -s /sbin/nologin flute

# Copy flute framework and worker handler code
COPY llming_flute/flute/ /app/flute/
COPY workers/ /app/workers/
RUN python -m compileall -q /app/flute/

WORKDIR /app

USER flute

ENV PYTHONDONTWRITEBYTECODE=1
ENV MPLCONFIGDIR=/tmp/matplotlib
ENV MPLBACKEND=Agg

CMD ["python", "-u", "-m", "flute.server"]
