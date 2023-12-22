# FROM golang:1.21.1
# WORKDIR /app
# COPY go.mod go.sum ./
# RUN go mod download

FROM golang:1.21.1 as builder
WORKDIR /build
COPY go.mod go.sum ./
RUN go mod download
COPY ./ ./
# RUN CGO_ENABLED=0 go build -o ./main
RUN go build scratchdata
EXPOSE 3000
ENTRYPOINT ["./scratchdata", "local.toml"]


# FROM public.ecr.aws/lambda/provided:al2023
# FROM scratch
# WORKDIR /app
# COPY --from=builder /build/scratchdata /app/
# # COPY --from=builder /build/scratchdata ./scratchdata
# COPY local.toml ./
# COPY storage_local.toml ./
# ENTRYPOINT ["/app/scratchdata", "local.toml"]
# ENTRYPOINT ["./certmaster", "lambda"]

# FROM scratch
# WORKDIR /app
# COPY local.toml ./
# COPY storage_local.toml ./
# COPY --from=builder /build/scratchdata ./scratchdata
# EXPOSE 3000
# ENTRYPOINT ["./scratchdata", "local.toml"]
