FROM golang:alpine AS build-env
COPY . /work
WORKDIR /work
RUN go build

FROM alpine
COPY --from=build-env /work/proxy /sxbin/proxy
WORKDIR /sxbin
