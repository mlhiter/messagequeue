// Release image build contract. Run from the repository root with:
//   docker buildx bake -f deploy/docker-bake.hcl --push
// The application Dockerfiles are added with each vertical slice. Keeping the
// platform here prevents an accidental ARM-only publish from a developer Mac.

variable "REGISTRY" {
  default = "crpi-7jr40k6elhldekqp.cn-hangzhou.personal.cr.aliyuncs.com/mlhiter"
}

variable "VERSION" {
  default = "v0.1.4"
}

group "default" {
  targets = ["controller", "backend", "frontend"]
}

target "common" {
  context = "."
  platforms = ["linux/amd64"]
  provenance = true
  sbom = true
}

target "controller" {
  inherits = ["common"]
  dockerfile = "controller/Dockerfile"
  tags = ["${REGISTRY}/messagequeue-controller:${VERSION}"]
}

target "backend" {
  inherits = ["common"]
  dockerfile = "backend/Dockerfile"
  tags = ["${REGISTRY}/messagequeue-backend:${VERSION}"]
}

target "frontend" {
  inherits = ["common"]
  dockerfile = "frontend/Dockerfile"
  tags = ["${REGISTRY}/messagequeue-frontend:${VERSION}"]
}
