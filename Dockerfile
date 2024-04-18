FROM golang:1.22.1 as builder
WORKDIR /build
COPY go.mod go.sum ./
COPY ./ ./
RUN go mod download
RUN go build -o scratchdata
EXPOSE 8080
ENTRYPOINT ["./scratchdata"]
