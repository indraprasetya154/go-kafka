# build
FROM golang:1.23.3-alpine as builder
WORKDIR /app
COPY . .
RUN CGO_ENABLED=0 go build -o bin/producer .

FROM gcr.io/distroless/static-debian12
COPY --from=builder /app/bin/producer /producer

CMD ["/producer"]
