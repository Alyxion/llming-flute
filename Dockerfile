FROM python:3.14-slim-bookworm

# Runtime libs: fonts for matplotlib, OpenMP for numpy/scipy
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        fonts-dejavu-core \
        libgomp1 && \
    rm -rf /var/lib/apt/lists/*

# Pre-install scientific Python stack
RUN pip install --no-cache-dir \
    numpy pandas matplotlib openpyxl pdfplumber \
    scipy pillow sympy psutil redis

# Non-root user with HOME=/tmp (writable via tmpfs)
RUN groupadd -r flute && useradd -r -g flute -d /tmp -s /sbin/nologin flute

COPY llming_flute/flute/ /app/flute/
RUN python -m compileall -q /app/flute/

WORKDIR /app
USER flute

ENV PYTHONDONTWRITEBYTECODE=1
ENV MPLCONFIGDIR=/tmp/matplotlib
ENV MPLBACKEND=Agg

CMD ["python", "-u", "-m", "flute.server"]
