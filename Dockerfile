FROM golang:1.21.1 as builder
WORKDIR /build
COPY go.mod go.sum ./
RUN go mod download
COPY ./ ./
RUN go build scratchdata
EXPOSE 3000
ENTRYPOINT ["./scratchdata", "local.toml"]