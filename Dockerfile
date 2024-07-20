FROM fedora:40

# Copy the requirements file
COPY env.txt /tmp/requirements.txt

# Install dependencies
RUN dnf install -y gcc \
                   make \
                   openssl-devel \
                   bzip2-devel \
                   libffi-devel \
                   zlib-devel \
                   xz-devel \
                   curl && \
    dnf clean all && \
    dnf autoremove -y && \
    rm -rf /var/cache/* && \
    rm -rf /var/log/dnf/*

# Install Python
RUN curl -sSL https://www.python.org/ftp/python/3.11.9/Python-3.11.9.tgz -o Python-3.11.9.tgz && \
    tar xvf Python-3.11.9.tgz && \
    cd Python-3.11.9 && \
    ./configure --enable-optimizations CC="gcc -pthread" CXX="g++ -pthread" && \
    make -j 24 && \
    make altinstall

# Remove existing symlinks if they exist
RUN rm -f /usr/bin/python3 && \
    rm -f /usr/bin/pip3

# Create new symlinks to the installed Python version
RUN ln -s /usr/local/bin/python3.11 /usr/bin/python3 && \
    ln -s /usr/local/bin/pip3.11 /usr/bin/pip3

# Install the requirements
RUN pip3 install -r /tmp/requirements.txt

CMD ["python3"]
