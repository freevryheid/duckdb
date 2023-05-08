FROM ubuntu:latest

ARG FPM_BIN="https://github.com/fortran-lang/fpm/releases/download/v0.7.0/fpm-0.7.0-linux-x86_64"
ARG DUCKDB_DISTR="https://github.com/duckdb/duckdb/releases/download/v0.7.1/libduckdb-linux-aarch64.zip"

RUN apt-get update && export DEBIAN_FRONTEND=noninteractive && \
    apt-get -y install --no-install-recommends \
    git \
    openssh-client \
    unzip \
    python3-pip \ 
    wget \
    gfortran-11 \
    valgrind

RUN pip3 install --no-input fortls fprettify

# Set gfortran-11 and gcc-11 as default 
RUN update-alternatives --install /usr/bin/gfortran gfortran /usr/bin/gfortran-11 11 && \
  update-alternatives --install /usr/bin/gcc gcc /usr/bin/gcc-11 11 
  
# Install fpm
RUN cd /tmp && \
  wget --no-check-certificate "${FPM_BIN}" && \ 
  chmod +x $(basename "${FPM_BIN}") && \
  cp $(basename ${FPM_BIN}) /usr/bin && \
  cd /usr/bin && \
  ln -sf /usr/bin/$(basename ${FPM_BIN}) fpm

# Download and install duckdb
RUN cd /tmp && \
    mkdir duckdb && \
    cd duckdb && \
    wget "${DUCKDB_DISTR}" && \
    unzip $(basename ${DUCKDB_DISTR}) && \
    mv *.h *.hpp /usr/local/include/ && \ 
    mv *.so /usr/local/lib/

ENV LD_LIBRARY_PATH=/usr/local/lib
