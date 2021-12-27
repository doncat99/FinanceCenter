FROM postgres:14.1
RUN sed -i 's/deb.debian.org/mirrors.aliyun.com/g' /etc/apt/sources.list; \
    apt update; \
    apt install -y libdbd-pg-perl libdbi-perl perl-modules
ADD ./postgresqltuner/postgresqltuner.pl /work/

