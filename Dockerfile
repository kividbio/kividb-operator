# Build the kividb-operator manager binary (main.go at repo root).
#
# Two-stage build: compile statically with CGO disabled in a full Go image,
# then copy just the binary into a distroless, non-root runtime image. This
# keeps the final image minimal (no shell, no package manager) and follows
# the standard kubebuilder/controller-runtime convention.

FROM golang:1.23-bookworm AS builder
WORKDIR /workspace

# Cache module downloads separately from source changes.
COPY go.mod go.mod
COPY go.sum go.sum
RUN go mod download

# Copy the rest of the source tree.
COPY main.go main.go
COPY api/ api/
COPY internal/ internal/

# Build a fully static binary: no CGO, so it runs unmodified on the
# distroless static base image below.
RUN CGO_ENABLED=0 GOOS=linux go build -a -o /manager .

FROM gcr.io/distroless/static-debian12:nonroot
WORKDIR /
COPY --from=builder /manager /manager
USER 65532:65532

ENTRYPOINT ["/manager"]
