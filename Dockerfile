FROM golang:1.9

# install fpm, git, gpg
RUN apt-get update && apt-get install -y --no-install-recommends \
        ruby \
        ruby-dev \
        gcc \
        libffi-dev \
        make \
        libc-dev \
        rpm \
        gnupg \
        git \
    && rm -rf /var/lib/apt/lists/* \
    && gem install --no-ri --no-rdoc fpm

# some gpg commands here, just to initialize the .gnupg directory.
RUN gpg --list-keys

WORKDIR /root
VOLUME "/root/.gnupg/"

CMD ["bash"]