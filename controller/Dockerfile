# syntax=docker/dockerfile:1.7
FROM --platform=$BUILDPLATFORM golang:1.23 AS build
ARG TARGETOS
ARG TARGETARCH
WORKDIR /src
COPY controller/go.mod controller/go.sum ./
RUN go mod download
COPY controller/ ./
RUN CGO_ENABLED=0 GOOS=${TARGETOS:-linux} GOARCH=${TARGETARCH:-amd64} go build -trimpath -ldflags='-s -w' -o /out/messagequeue-controller ./cmd

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/messagequeue-controller /messagequeue-controller
USER 65532:65532
ENTRYPOINT ["/messagequeue-controller"]
