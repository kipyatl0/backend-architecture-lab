# Один Dockerfile на все сервисы стенда: BIN выбирает, какой собрать.
# Язык реализации — деталь стенда, а не предмет курса; в контенте он не
# называется никогда, здесь называется свободно.

FROM golang:1.25-alpine AS build
ARG BIN
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags "-s -w" -o /out/service ./cmd/${BIN}

FROM alpine:3.22
RUN apk add --no-cache ca-certificates tzdata wget
COPY --from=build /out/service /usr/local/bin/service
ENV TZ=UTC
ENTRYPOINT ["/usr/local/bin/service"]
