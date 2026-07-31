# syntax=docker/dockerfile:1.7
FROM --platform=$BUILDPLATFORM golang:1.23-alpine AS build
ARG TARGETOS=linux
ARG TARGETARCH=amd64
WORKDIR /src
COPY backend/go.mod backend/go.sum ./
RUN go mod download
COPY backend/*.go ./
RUN CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH go build -trimpath -ldflags='-s -w' -o /out/messagequeue-backend .

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/messagequeue-backend /messagequeue-backend
USER 65532:65532
EXPOSE 8080
ENTRYPOINT ["/messagequeue-backend"]
