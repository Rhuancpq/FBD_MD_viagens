FROM postgres:14-alpine

COPY ./script.sql /docker-entrypoint-initdb.d/