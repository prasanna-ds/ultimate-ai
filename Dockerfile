FROM ubuntu:bionic

# Install python3.8
RUN apt-get update \
    && apt-get install -y wget \
    && DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
        software-properties-common \
    && add-apt-repository -y ppa:deadsnakes \
    && DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
        python3.8-venv \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

RUN python3.8 -m venv /venv
ENV PATH=/venv/bin:$PATH

# Set ENVs for python, pip and Install Poetry
ENV PYTHONFAULTHANDLER=1 \
  PYTHONUNBUFFERED=1 \
  PYTHONHASHSEED=random \
  PIP_NO_CACHE_DIR=off \
  PIP_DISABLE_PIP_VERSION_CHECK=on \
  PIP_DEFAULT_TIMEOUT=100 \
  POETRY_VERSION=1.1.6

RUN pip install "poetry==$POETRY_VERSION"

# Install Java
RUN apt-get update && \
    apt-get install -y openjdk-11-jre-headless && \
    apt-get clean

# Install Spark
ENV DAEMON_RUN=true
ENV SPARK_VERSION=3.0.3
ENV HADOOP_VERSION=3.2
ENV SPARK_HOME=/spark

RUN wget --no-verbose http://apache.mirror.iphh.net/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz \
      && tar -xvzf spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz \
      && mv spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} spark \
      && rm spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz

# Copy application code
RUN mkdir app
ADD twitter_simulator /app/twitter_simulator/
ADD stream_processor /app/stream_processor/
COPY main.py processors.py pyproject.toml poetry.lock /app/
COPY start.sh /app/
RUN chmod +x /app/start.sh

# Set working directory
WORKDIR app

# Install Application dependencies
RUN poetry install --no-dev
RUN pip install beautifulsoup4

CMD ["/app/start.sh"]