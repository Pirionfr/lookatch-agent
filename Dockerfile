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
        expect \
        pinentry-tty \
    && rm -rf /var/lib/apt/lists/* \
    && gem install --no-ri --no-rdoc fpm

RUN gpg --list-keys \
    && echo "pinentry-program /usr/bin/pinentry-tty" > ~/.gnupg/gpg-agent.conf \
    && echo "allow-preset-passphrase" >> ~/.gnupg/gpg-agent.conf



RUN gpgconf --kill gpg-agent \
    && eval $(gpg-agent --daemon --allow-preset-passphrase --default-cache-ttl 1 --max-cache-ttl 31536000)

ENV PATH="/usr/lib/gnupg:${PATH}"

CMD []