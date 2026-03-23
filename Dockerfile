ARG GO_VERSION=1.25

FROM gcr.io/distroless/base as distroless

FROM golang:${GO_VERSION}-bookworm AS compile

ARG VERSION=dev

USER root

RUN mkdir -p /src/app

WORKDIR /src/app

COPY go.mod go.sum ./

RUN go mod download

COPY pkg ./pkg

COPY main.go ./main.go

ENV GOCACHE=/root/.cache/go-build

RUN mkdir -p /root/.cache/go-build

RUN --mount=type=cache,target="/root/.cache/go-build",rw CGO_ENABLED=0 go build \
    -ldflags "-s -w -X github.com/d8a-tech/d8a/pkg/cmd.version=${VERSION}" \
    -o /home/go/app ./main.go


RUN mkdir -p /storage/queue /storage/spool /storage/currency /storage/dbip && chown -R 1000:1000 /storage

# Prod stage, wraps build stage result in distroless image
FROM distroless AS prod

ARG VERSION=dev

ENV VERSION=${VERSION}

ENV GIN_MODE=release
ENV STORAGE_BOLT_DIRECTORY=/storage
ENV STORAGE_QUEUE_DIRECTORY=/storage/queue
ENV STORAGE_SPOOL_DIRECTORY=/storage/spool
ENV CURRENCY_DESTINATION_DIRECTORY=/storage/currency
ENV DBIP_DESTINATION_DIRECTORY=/storage/dbip



USER 1000:1000

COPY --from=compile --chown=1000:1000 /home/go/app /bin/app
COPY --from=compile --chown=1000:1000 /storage /storage

ENTRYPOINT ["/bin/app"]

CMD ["server"]
