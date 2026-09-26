FROM golang:1.22.12-alpine AS build
WORKDIR /app
COPY . .
RUN go test ./... && CGO_ENABLED=0 go build -o /products .
FROM alpine:3.21
COPY --from=build /products /products
USER 10001
EXPOSE 8081
ENTRYPOINT ["/products"]
