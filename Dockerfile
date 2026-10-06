# Môi trường test local + CI, khớp Fabric Runtime 2.0: Java 21, Python 3.13, Spark 4.1.1, Delta 4.2.0
FROM eclipse-temurin:21-jre-noble

COPY --from=ghcr.io/astral-sh/uv:0.12.7 /uv /uvx /bin/

ENV UV_PROJECT_ENVIRONMENT=/opt/venv \
    UV_PYTHON_INSTALL_DIR=/opt/python \
    UV_LINK_MODE=copy \
    UV_COMPILE_BYTECODE=1 \
    PATH="/opt/venv/bin:$PATH"

WORKDIR /app

# Lớp phụ thuộc: chỉ build lại khi pyproject/uv.lock đổi
COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-install-project

# Tải sẵn jar Delta (Ivy) vào image → test không phải tải mạng mỗi lần chạy
COPY tests/conftest.py /tmp/warmup/conftest.py
RUN cd /tmp/warmup && python -c "from conftest import build_spark; build_spark('/tmp/warmup/wh').stop()"

CMD ["pytest"]
