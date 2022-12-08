FROM jupyter/all-spark-notebook

USER root
COPY target/py-dist/pathling-*.whl /tmp

USER ${NB_UID}

RUN pip install --no-cache-dir -i https://pypi.mirrors.ustc.edu.cn/simple/ /tmp/pathling-*.whl && \
    fix-permissions "${CONDA_DIR}" && \
    fix-permissions "/home/${NB_USER}"
