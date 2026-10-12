# make docker-build first compiles bin/manager with CGO_ENABLED=0.
# The Kubernetes API CA is supplied by the mounted service account.
FROM scratch
COPY bin/manager /manager
USER 65532:65532
ENTRYPOINT ["/manager"]
