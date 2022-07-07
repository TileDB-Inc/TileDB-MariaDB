FROM ubuntu:20.04
LABEL maintainer="support@tiledb.io"

ENV AWS_EC2_METADATA_DISABLED true

ENV DEBIAN_FRONTEND=noninteractive
ENV TZ=GMT

RUN apt-get update && apt-get install -y \
  gosu \
  pwgen \
  tzdata \
  gcc \
  g++ \
  build-essential \
  libasan5 \
  bison \
  chrpath \
  cmake \
  gdb \
  gnutls-dev \
  libaio-dev \
  libboost-dev \
  libdbd-mysql \
  libjudy-dev \
  libncurses5-dev \
  libpam0g-dev \
  libpcre3-dev \
  libreadline-gplv2-dev \
  libstemmer-dev \
  libssl-dev \
  libnuma-dev \
  libxml2-dev \
  lsb-release \
  perl \
  psmisc \
  zlib1g-dev \
  libcrack2-dev \
  cracklib-runtime \
  libjemalloc-dev \
  libsnappy-dev \
  liblzma-dev \
  libzmq3-dev \
  uuid-dev \
  ccache \
  git \
  wget \
  libcurl4-openssl-dev \
  libcurl4 \
  && rm -rf /var/lib/apt/lists/*

ENV MTR_MEM /tmp

WORKDIR /tmp

ENV MARIADB_VERSION="mariadb-10.5.13"

ARG MYTILE_VERSION="0.17.1"

ARG TILEDB_VERSION="2.10.2"
ARG TILEDB_VERSION_SHORT_SHA="9ab84f9"
ARG TILEDB_PREBUILT_FILE="tiledb-linux-x86_64-${TILEDB_VERSION}-${TILEDB_VERSION_SHORT_SHA}.tar.gz"

# Download mytile release
RUN wget https://github.com/TileDB-Inc/TileDB-MariaDB/archive/${MYTILE_VERSION}.tar.gz -O /tmp/${MYTILE_VERSION}.tar.gz \
  && tar xf /tmp/${MYTILE_VERSION}.tar.gz \
  && mv TileDB-MariaDB-${MYTILE_VERSION} mytile

# Copy example arrays to opt
RUN cp -r /tmp/mytile/mysql-test/mytile/test_data/tiledb_arrays /opt/

# add our user and group first to make sure their IDs get assigned consistently, regardless of whatever dependencies get added
RUN groupadd -r mysql && useradd -r -g mysql mysql

# Set git config so superbuild of tiledb can cherry-pick capnp fix
RUN git config --global user.email "you@example.com" \
 && git config --global user.name "Your Name"

# Install tiledb using 2.4 release
RUN wget https://github.com/TileDB-Inc/TileDB/releases/download/${TILEDB_VERSION}/${TILEDB_PREBUILT_FILE} \
 && tar xf ${TILEDB_PREBUILT_FILE} -C /usr \
 && rm ${TILEDB_PREBUILT_FILE} \
 && ldconfig

RUN cp /tmp/mytile/docker/my.cnf /etc/mysql/my.cnf \
 && cp /tmp/mytile/docker/mytile.cnf /etc/mysql/conf.d/mytile.cnf \
 && cp /tmp/mytile/docker/docker-entrypoint-interactive.sh /usr/local/bin/docker-entrypoint-interactive.sh

RUN cp /tmp/mytile/docker/init.sh /tmp/init.sh

ENV CXXFLAGS="${CXXFLAGS} -Wno-error=deprecated-declarations"
ENV CFLAGS="${CFLAGS} -Wno-error=deprecated-declarations"

RUN wget https://downloads.mariadb.org/interstitial/${MARIADB_VERSION}/source/${MARIADB_VERSION}.tar.gz \
  && tar xf ${MARIADB_VERSION}.tar.gz \
  && mv /tmp/mytile ${MARIADB_VERSION}/storage/mytile \
  && cd ${MARIADB_VERSION} \
  && mkdir builddir \
  && cd builddir \
  && cmake -DPLUGIN_INNODB=NO -DPLUGIN_TOKUDB=NO -DPLUGIN_ROCKSDB=NO -DPLUGIN_MROONGA=NO -DPLUGIN_SPIDER=NO -DPLUGIN_SPHINX=NO -DPLUGIN_FEDERATED=NO -DPLUGIN_FEDERATEDX=NO -DPLUGIN_CONNECT=NO -DCMAKE_BUILD_TYPE=Release -SWITH_DEBUG=0 -DBUILD_CONFIG=mysql_release -DWITH_EMBEDDED_SERVER=OFF -DCMAKE_INSTALL_PREFIX=/opt/server .. \
  && make -j$(nproc) \
  && make install \
  && cd ../../ \
  && rm -r ${MARIADB_VERSION}


# the "/var/lib/mysql" stuff here is because the mysql-server postinst doesn't have an explicit way to disable the mysql_install_db codepath besides having a database already "configured" (ie, stuff in /var/lib/mysql/mysql)
# also, we set debconf keys to make APT a little quieter
RUN set -ex; \
# comment out any "user" entires in the MySQL config ("docker-entrypoint.sh" or "--user" will handle user switching)
	sed -ri 's/^user\s/#&/' /etc/mysql/my.cnf /etc/mysql/conf.d/*; \
# purge and re-create /var/lib/mysql with appropriate ownership
	rm -rf /var/lib/mysql; \
	mkdir -p /var/lib/mysql /var/run/mysqld; \
	chown -R mysql:mysql /var/lib/mysql /var/run/mysqld; \
# ensure that /var/run/mysqld (used for socket and lock files) is writable regardless of the UID our mysqld instance ends up having at runtime
	chmod 777 /var/run/mysqld; \
# comment out a few problematic configuration values
	find /etc/mysql/ -name '*.cnf' -print0 \
		| xargs -0 grep -lZE '^(bind-address|log)' \
		| xargs -rt -0 sed -Ei 's/^(bind-address|log)/#&/'; \
# don't reverse lookup hostnames, they are usually another container
	echo '[mysqld]\nskip-host-cache\nskip-name-resolve' > /etc/mysql/conf.d/docker.cnf

RUN chmod +x /usr/local/bin/docker-entrypoint-interactive.sh

RUN ln -s usr/local/bin/docker-entrypoint-interactive.sh / # backwards compat

ENV PATH="${PATH}:/opt/server/bin:/opt/server/scripts"

RUN mkdir /var/log/mysql

RUN chown -R mysql:mysql /var/log/mysql

RUN chown -R mysql:mysql /opt/server

RUN chown -R mysql:mysql /opt/tiledb_arrays

# Set defaults for initializing the server
ENV MYSQL_ALLOW_EMPTY_PASSWORD=1

ENV MYSQL_DATABASE=test

# Set paths needed for bitnami helm mariadb
RUN mkdir /opt/bitnami
RUN ln -s /opt/server /opt/bitnami/mariadb

# Setup initial DB for faster starts
RUN /tmp/init.sh "mysqld"

USER mysql

EXPOSE 3306

CMD ["docker-entrypoint-interactive.sh", "mysqld"]
