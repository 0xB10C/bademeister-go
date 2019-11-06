# inspired by https://github.com/ruimarinho/docker-bitcoin-core/blob/master/0.18/Dockerfile
FROM debian:stable-slim

RUN groupadd -r bitcoin && useradd -r -m -g bitcoin bitcoin

RUN apt-get update
RUN apt-get install -y \
      build-essential \
      autoconf \
      libtool \
      pkg-config \
      libboost-all-dev \
      libssl-dev \
      libprotobuf-dev \
      protobuf-compiler \
      libevent-dev \
      libqt4-dev \
      libcanberra-gtk-module \
      libdb-dev \
      libdb++-dev \
      bsdmainutils \
      wget
RUN rm -rf /var/lib/apt/lists/*

ENV BITCOIN_VERSION 0.18.99-dirtyf03785b4e
ENV BITCOIN_URL https://github.com/0xB10C/bitcoin/archive/v${BITCOIN_VERSION}.tar.gz

#ENV BITCOIN_SHA256 600d1db5e751fa85903e935a01a74f5cc57e1e7473c15fd3e17ed21e202cfe5a 
#ENV BITCOIN_ASC_URL https://bitcoin.org/bin/bitcoin-core-0.18.1/SHA256SUMS.asc
#ENV BITCOIN_PGP_KEY 01EA5486DE18A882D4C2684590C8019E36C2E964

# COPY v$BITCOIN_VERSION.tar.gz /tmp/bitcoin.tar.gz
#
RUN set -ex \
  && cd /tmp \
  && wget -qO bitcoin.tar.gz "$BITCOIN_URL" \
  && tar -xzvf bitcoin.tar.gz \
  && cd "/tmp/bitcoin-$BITCOIN_VERSION" \
  && ./autogen.sh \
  && ./configure --with-incompatible-bdb \
  && make \
  && make install

CMD bash

# # create data directory
# ENV BITCOIN_DATA /data
# RUN mkdir "$BITCOIN_DATA" \
#   && chown -R bitcoin:bitcoin "$BITCOIN_DATA" \
#   && ln -sfn "$BITCOIN_DATA" /home/bitcoin/.bitcoin \
#   && chown -h bitcoin:bitcoin /home/bitcoin/.bitcoin
# VOLUME /data
# 
# COPY docker-entrypoint.sh /entrypoint.sh
# ENTRYPOINT ["/entrypoint.sh"]
# 
# EXPOSE 8332 8333 18332 18333
# CMD ["bitcoind"]
