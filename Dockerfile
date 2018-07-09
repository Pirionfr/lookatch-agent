FROM golang:latest



# install fpm, git, gpg
RUN apt-get install -y --no-install-recommends \
        ruby \
        ruby-dev \
        gcc \
        libffi-dev \
        make \
        libc-dev \
        rpm \
        gnupg \
        git \
    && gem install --no-ri --no-rdoc fpm

