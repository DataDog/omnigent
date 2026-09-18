# Consolidated-main delivery overlay

This overlay makes the consolidated DataDog `main` self-delivering. Dynamic
Build builds the checked-out commit into a signed multi-architecture image, then `ddr-package`
publishes a bundle that injects that image's immutable digest into the colocated
Helm chart. No `ddoghq/images` update or `k8s-resources` image-tag bump is part
of this flow.

`k8s/omnigent-server` is the canonical chart. It owns Habitat configuration,
PostgreSQL, and service/Fabric behavior. The only release-image adaptation is
`cnab.images.main`; Conductor supplies its registry, repository, tag, and
digest when it packages the bundle. Retire the old images and k8s-resources
definitions after this rollout is established.

## Keeping the overlay current

When `main` changes Docker build inputs, update `Dockerfile` while retaining
the no-clone rule, frozen lockfile installation, and immutable
`omnigent-hab-launcher` install. Preserve the `cnab.images.main` image
expression, then run `helm lint` and `helm template`. Do not restore a fixed
Omnigent image tag.
