FROM ghcr.io/actions/actions-runner:latest

USER root

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    dirmngr \
    gnupg && \
    apt-key adv --keyserver keyserver.ubuntu.com --recv-keys E298A3A825C0D65DFD57CBB651716619E084DAB9 && \
    add-apt-repository "deb https://cloud.r-project.org/bin/linux/ubuntu $(lsb_release -cs)-cran40/" && \
    apt-get update && \
    apt-get install -y --no-install-recommends \
    libcurl4-openssl-dev \
    r-base \
    texlive-fonts-extra \
    texlive-latex-base && \
    apt-get clean

USER runner
