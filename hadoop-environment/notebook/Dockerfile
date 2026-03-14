FROM hadoop-base:3.3.6

USER root

RUN pip install --no-cache-dir jupyterlab hdfs

WORKDIR /workspace

EXPOSE 8888

CMD ["jupyter", "lab", "--ip=0.0.0.0", "--port=8888", "--no-browser", "--allow-root", "--IdentityProvider.token=", "--PasswordIdentityProvider.password="]
