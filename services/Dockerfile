# Один Dockerfile на все сервисы стенда: BIN выбирает, какой собрать.
# Язык реализации — деталь стенда, а не предмет курса; в контенте он не
# называется никогда, здесь называется свободно.

# Сборка идёт на родной архитектуре и кросс-компилируется под целевую:
# эмуляция здесь стоила бы минуты на каждый образ и ничего не давала.
FROM --platform=$BUILDPLATFORM golang:1.25-alpine AS build
ARG BIN
ARG TARGETOS
ARG TARGETARCH
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} \
    go build -trimpath -ldflags "-s -w" -o /out/service ./cmd/${BIN}

FROM alpine:3.22
RUN apk add --no-cache ca-certificates tzdata wget
COPY --from=build /out/service /usr/local/bin/service
ENV TZ=UTC
ENTRYPOINT ["/usr/local/bin/service"]
