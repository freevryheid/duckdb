FROM ubuntu:latest

ARG FPM_BIN="https://github.com/fortran-lang/fpm/releases/download/v0.7.0/fpm-0.7.0-linux-x86_64"
ARG DUCKDB_DISTR="https://github.com/duckdb/duckdb/releases/download/v0.8.1/libduckdb-linux-aarch64.zip"
ARG GIT_PROMPT="https://raw.githubusercontent.com/git/git/master/contrib/completion/git-prompt.sh"
ARG GIT_COMPLETION="https://raw.githubusercontent.com/git/git/master/contrib/completion/git-completion.bash"

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

RUN wget --no-check-certificate -O ~/.git-prompt.sh "${GIT_PROMPT}" && \
    wget --no-check-certificate -O ~/.git-completion.bash "${GIT_COMPLETION}" && \
    echo "source ~/.git-completion.bash" >> ~/.bashrc && \
    echo "source ~/.git-prompt.sh" >> ~/.bashrc && \
    echo "export GIT_PS1_SHOWDIRTYSTATE=1" >> ~/.bashrc && \
    echo "export GIT_PS1_SHOWUNTRACKEDFILES=1" >> ~/.bashrc && \
    echo "export GIT_PS1_SHOWCOLORHINTS=1" >> ~/.bashrc && \
    echo "export PROMPT_COMMAND='__git_ps1 \"\u@\h:\W\" \"\\\$ \"'" >> ~/.bashrc

