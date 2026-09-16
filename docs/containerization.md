# Containerization

## Purpose

The project includes a multi-stage Docker build that provides:

- A reproducible Python 3.12 environment
- Isolated dependency installation
- Containerized linting and tests
- A smaller runtime image
- Non-root execution
- Persistent data through mounted storage
- Support for Linux ARM64 and AMD64-compatible base images

## Files

```text
Dockerfile
.dockerignore