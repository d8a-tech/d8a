ARG GO_VERSION=1.25

FROM gcr.io/distroless/base as distroless

FROM golang:${GO_VERSION}-bookworm AS compile

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
    -o /home/go/app ./main.go

# Prod stage, wraps build stage result in distroless image
FROM distroless AS prod

ARG VERSION=dev

ENV VERSION=${VERSION}

ENV GIN_MODE=release

USER 1000:1000

COPY --from=compile --chown=1000:1000 /home/go/app /bin/app

ENTRYPOINT ["/bin/app"]

CMD ["server"]

