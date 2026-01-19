# Pathling CI Runner Docker Image
#
# This image contains all dependencies required to build and test Pathling
# in CI environments.
#
# @author John Grimes

FROM ubuntu:24.04

LABEL org.opencontainers.image.source="https://github.com/aehrc/pathling"
LABEL org.opencontainers.image.description="CI runner image for Pathling builds"
LABEL org.opencontainers.image.licenses="Apache-2.0"

# Use bash with pipefail to catch errors in piped commands.
SHELL ["/bin/bash", "-o", "pipefail", "-c"]

# Avoid interactive prompts during package installation.
ENV DEBIAN_FRONTEND=noninteractive

# Set locale to UTF-8.
ENV LANG=en_US.UTF-8
ENV LANGUAGE=en_US:en
ENV LC_ALL=en_US.UTF-8

# Install base dependencies and locale support.
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    curl \
    wget \
    perl \
    gnupg \
    lsb-release \
    software-properties-common \
    locales \
    git \
    unzip \
    zip \
    xz-utils \
    build-essential \
    && locale-gen en_US.UTF-8 \
    && rm -rf /var/lib/apt/lists/*

# =============================================================================
# Java: Azul Zulu JDK 21
# =============================================================================
RUN curl -fsSL https://repos.azul.com/azul-repo.key | gpg --dearmor -o /usr/share/keyrings/azul.gpg \
    && echo "deb [signed-by=/usr/share/keyrings/azul.gpg] https://repos.azul.com/zulu/deb stable main" > /etc/apt/sources.list.d/zulu.list \
    && apt-get update \
    && apt-get install -y --no-install-recommends zulu21-jdk \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/zulu21
ENV PATH="${JAVA_HOME}/bin:${PATH}"

# =============================================================================
# Maven 3.9.9
# =============================================================================
ARG MAVEN_VERSION=3.9.9
ENV MAVEN_HOME=/opt/maven
ENV PATH="${MAVEN_HOME}/bin:${PATH}"

RUN curl -fsSL "https://archive.apache.org/dist/maven/maven-3/${MAVEN_VERSION}/binaries/apache-maven-${MAVEN_VERSION}-bin.tar.gz" \
    | tar -xz -C /opt \
    && mv "/opt/apache-maven-${MAVEN_VERSION}" "${MAVEN_HOME}"

# Maven options for Java 21 compatibility.
ENV MAVEN_OPTS="--add-exports=java.base/sun.nio.ch=ALL-UNNAMED --add-opens=java.base/java.net=ALL-UNNAMED"

# =============================================================================
# Python 3 and uv
# =============================================================================
RUN apt-get update && apt-get install -y --no-install-recommends \
    python3 \
    python3-pip \
    python3-venv \
    && rm -rf /var/lib/apt/lists/*

# Install uv (Python package manager).
RUN curl -LsSf https://astral.sh/uv/install.sh -o /tmp/uv-install.sh \
    && sh /tmp/uv-install.sh \
    && rm /tmp/uv-install.sh
ENV PATH="/root/.local/bin:${PATH}"

# =============================================================================
# R and dependencies
# =============================================================================
# Install R dependencies.
RUN apt-get update && apt-get install -y --no-install-recommends \
    libcurl4-openssl-dev \
    libssl-dev \
    libxml2-dev \
    libfontconfig1-dev \
    libfreetype6-dev \
    libpng-dev \
    libtiff5-dev \
    libjpeg-dev \
    libharfbuzz-dev \
    libfribidi-dev \
    && rm -rf /var/lib/apt/lists/*

# Add CRAN repository for R.
RUN curl -fsSL https://cloud.r-project.org/bin/linux/ubuntu/marutter_pubkey.asc | gpg --dearmor -o /usr/share/keyrings/r-project.gpg \
    && echo "deb [signed-by=/usr/share/keyrings/r-project.gpg] https://cloud.r-project.org/bin/linux/ubuntu $(lsb_release -cs)-cran40/" > /etc/apt/sources.list.d/r-project.list

# Install R.
RUN apt-get update && apt-get install -y --no-install-recommends \
    r-base \
    r-base-dev \
    && rm -rf /var/lib/apt/lists/*

# Install Pandoc.
ARG PANDOC_VERSION=3.1.13
RUN curl -fsSL "https://github.com/jgm/pandoc/releases/download/${PANDOC_VERSION}/pandoc-${PANDOC_VERSION}-linux-amd64.tar.gz" \
    | tar -xz -C /opt \
    && ln -s "/opt/pandoc-${PANDOC_VERSION}/bin/pandoc" /usr/local/bin/pandoc

# Install TinyTeX for R package documentation.
RUN curl -fsSL https://yihui.org/tinytex/install-bin-unix.sh -o /tmp/tinytex-install.sh \
    && cd /tmp && sh tinytex-install.sh \
    && rm /tmp/tinytex-install.sh \
    && find /root/.TinyTeX/bin -name tlmgr -exec {} path add \;
ENV PATH="/root/.TinyTeX/bin/x86_64-linux:${PATH}"

# =============================================================================
# Bun and Playwright
# =============================================================================
RUN curl -fsSL https://bun.sh/install -o /tmp/bun-install.sh \
    && bash /tmp/bun-install.sh \
    && rm /tmp/bun-install.sh
ENV PATH="/root/.bun/bin:${PATH}"

# Install Playwright and Chromium browser.
RUN bun install -g playwright \
    && bunx playwright install chromium --with-deps

# =============================================================================
# Helm
# =============================================================================
RUN curl -fsSL https://raw.githubusercontent.com/helm/helm/main/scripts/get-helm-3 -o /tmp/get-helm.sh \
    && bash /tmp/get-helm.sh \
    && rm /tmp/get-helm.sh

# =============================================================================
# Trivy (vulnerability scanner)
# =============================================================================
ARG TRIVY_VERSION=0.58.2
RUN curl -fsSL "https://github.com/aquasecurity/trivy/releases/download/v${TRIVY_VERSION}/trivy_${TRIVY_VERSION}_Linux-64bit.tar.gz" \
    | tar -xz -C /usr/local/bin trivy

# =============================================================================
# ORAS (OCI Registry As Storage)
# =============================================================================
ARG ORAS_VERSION=1.2.2
RUN curl -fsSL "https://github.com/oras-project/oras/releases/download/v${ORAS_VERSION}/oras_${ORAS_VERSION}_linux_amd64.tar.gz" \
    | tar -xz -C /usr/local/bin oras

# =============================================================================
# AWS CLI v2
# =============================================================================
RUN curl -fsSL "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o /tmp/awscliv2.zip \
    && unzip -q /tmp/awscliv2.zip -d /tmp \
    && /tmp/aws/install \
    && rm -rf /tmp/aws /tmp/awscliv2.zip

# =============================================================================
# GitHub CLI
# =============================================================================
RUN curl -fsSL https://cli.github.com/packages/githubcli-archive-keyring.gpg -o /usr/share/keyrings/githubcli-archive-keyring.gpg \
    && chmod go+r /usr/share/keyrings/githubcli-archive-keyring.gpg \
    && echo "deb [arch=$(dpkg --print-architecture) signed-by=/usr/share/keyrings/githubcli-archive-keyring.gpg] https://cli.github.com/packages stable main" > /etc/apt/sources.list.d/github-cli.list \
    && apt-get update \
    && apt-get install -y --no-install-recommends gh \
    && rm -rf /var/lib/apt/lists/*

# =============================================================================
# Final cleanup
# =============================================================================
RUN apt-get clean && rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*

WORKDIR /workspace

CMD ["/bin/bash"]
