"""Structural regressions for the consolidated-main delivery overlay."""

from __future__ import annotations

import subprocess
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[2]


def test_repository_keeps_mergegate_and_enables_dynamic_build() -> None:
    """Dynamic Build is additive; it must not replace the MergeGate."""
    documents = list(yaml.safe_load_all((ROOT / "repository.datadog.yml").read_text()))

    assert documents[0]["kind"] == "mergegate"
    assert documents[0]["rules"] == [
        {"require": "commit-signatures", "allow_unsigned_external": True}
    ]
    assert documents[1] == {
        "schema-version": "v1",
        "kind": "dynamicbuild-worker",
        "team": "workspaces",
        "rules_files": [".dynamic-build.yml"],
    }


def test_chart_uses_conductor_digest_injection() -> None:
    """The release image comes from the bundle, never a fixed image tag."""
    values = yaml.safe_load((ROOT / "k8s/omnigent-server/values.yaml").read_text())
    statefulset = (ROOT / "k8s/omnigent-server/templates/statefulset.yaml").read_text()

    assert "image" not in values
    assert values["cnab"]["images"]["main"]["digest"] == "PLACEHOLDER"
    assert 'image: "{{ $imageRegistry }}/{{ $imageRepository }}@{{ $imageDigest }}"' in statefulset
    assert "$.Values.image." not in statefulset


def test_conductor_targets_consolidated_main_manually() -> None:
    """Staging is a deliberate, main-only Conductor target."""
    service = yaml.safe_load((ROOT / "k8s/omnigent-server/service.datadog.yaml").read_text())
    target = service["extensions"]["datadoghq.com/sdp"]["conductor"]["targets"][0]

    assert target["branch"] == "main"
    assert target["schedule"] == "manual"
    assert target["slack"] == "workspaces-conductor-ops"
    assert target["ci_pipeline"] == "omnigent-server"
    assert target["deploy_config"]["ordered_deploy_targets"] == ["staging"]


def test_release_template_publishes_only_signed_digests_for_ddr() -> None:
    """PR verification cannot publish; DDR receives a digest-qualified image."""
    template = (ROOT / ".gitlab/ci/release.yml").read_text()

    assert "--platform linux/amd64,linux/arm64" in template
    assert "$CI_COMMIT_BRANCH == $DDCI_DEFAULT_BRANCH" in template
    assert "if: '$DDR_WORKFLOW_ID'" in template
    assert 'IMAGE_REF="${REGISTRY}/${IMAGE_REPOSITORY}@${IMAGE_DIGEST}"' in template
    assert "DDR_WORKFLOW_ID is required for Conductor publication" in template
    assert "--deploy-config-path k8s/omnigent-server --flavor staging" in template
    assert "SERVICE_NAME: {{.Rule.Variables.SERVICE_NAME}}" in template
    assert '--build-arg SOURCE_URL="${CI_PROJECT_URL}"' in template
    assert '--build-arg VCS_REF="${CI_COMMIT_SHA}"' in template
    rendered = yaml.safe_load(
        template.replace("{{.Rule.Variables.SERVICE_NAME}}", "omnigent-server")
    )
    assert rendered["variables"]["DOCKER_IMAGE"] == "${REGISTRY}/docker:27.3.1"
    assert (
        rendered["variables"]["BUNDLER_IMAGE"]
        == "${REGISTRY}/mini-repo-ci-image/ddr-package:v0.30"
    )
    jobs = [
        definition
        for name, definition in rendered.items()
        if name.startswith(
            ("verify-release-input:", "build-and-sign:", "publish-conductor-bundle:")
        )
    ]
    assert len(jobs) == 3
    assert all(job["tags"] == ["arch:amd64"] for job in jobs)


def test_dynamic_build_watches_every_docker_build_input() -> None:
    """A source change that can alter the image must schedule its pipeline."""
    changed = yaml.safe_load((ROOT / ".dynamic-build.yml").read_text())["omnigent-server"][
        "changed"
    ]

    assert {
        "Dockerfile",
        ".dockerignore",
        "package.json",
        "pyproject.toml",
        "setup.py",
        "uv.lock",
        "README.md",
        "LICENSE",
        "NOTICE",
    } <= set(changed)


def test_dockerignore_keeps_build_context_clean_without_hiding_inputs() -> None:
    """The checkout image never receives local state or generated web assets."""
    dockerignore = (ROOT / ".dockerignore").read_text().splitlines()

    for excluded in (
        ".git",
        ".venv",
        "**/node_modules",
        "omnigent/server/static/web-ui",
        "**/__pycache__",
        "build",
        "dist",
        ".pytest_cache",
    ):
        assert excluded in dockerignore
    assert "omnigent" not in dockerignore
    assert "sdks" not in dockerignore
    assert "web" not in dockerignore


def test_delivery_dockerfile_is_frozen_and_runtime_complete() -> None:
    """The GBI image has pinned inputs and retains editable source paths."""
    dockerfile = (ROOT / "Dockerfile").read_text()

    assert "registry.ddbuild.io/images/nodejs:24.13.1-gbi-noble" in dockerfile
    assert "registry.ddbuild.io/images/python:3.12.8" in dockerfile
    assert "COPY pyproject.toml setup.py uv.lock" in dockerfile
    assert "ARG UV_VERSION=0.7.19" in dockerfile
    assert 'python -m pip install --no-cache-dir "uv==${UV_VERSION}"' in dockerfile
    assert "uv sync --frozen --no-dev" in dockerfile
    assert "--extra databricks" not in dockerfile
    assert "uv pip install --python /opt/venv/bin/python 'psycopg[binary]>=3.1,<4'" in dockerfile
    assert "COPY --from=python-builder --chown=501:0 /src /src" in dockerfile
    assert (
        "https://binaries.ddbuild.io/dd-source/python/omnigent_hab_launcher-0.0.132434155-py3-none-any.whl"
        in dockerfile
    )
    assert "omnigent_hab_launcher-0.0.132434155-py3-none-any.whl" in dockerfile
    assert "375bffc76904a919bb6953333909535ba94e131b20842a8132d56dbd23d339ef" in dockerfile
    assert "sha256sum --check" in dockerfile
    assert "uv pip install --python /opt/venv/bin/python /tmp/omnigent_hab_launcher" in dockerfile
    assert "--no-deps" not in dockerfile
    assert "uv pip check --python /opt/venv/bin/python" in dockerfile
    assert "/opt/venv/bin/python -c 'import omnigent_hab_launcher'" in dockerfile
    assert "openssh-client" in dockerfile
    assert "install -d -o 501 -g 0 /data/artifacts" in dockerfile
    assert "USER 501" in dockerfile
    assert "org.opencontainers.image.revision" in dockerfile
    assert "OMNIGENT_SOURCE_REVISION" in dockerfile
    assert "git clone" not in dockerfile
    assert "git fetch" not in dockerfile


def test_patch_inventory_is_stable_and_actionable() -> None:
    """The delivery exception has a stable owner and review contract."""
    inventory = yaml.safe_load((ROOT / ".datadog/patches.yaml").read_text())["patches"][0]

    assert inventory["id"] == "OMNI-DELIVERY-001"
    assert inventory["owner"] == "workspaces"
    assert inventory["status"] == "datadog-only"
    assert inventory["required_tests"]
    assert inventory["conflict_resolution_notes"]


def test_chart_fails_closed_without_digest_or_single_replica() -> None:
    """Digest injection and Habitat's single-replica guard are render gates."""
    chart = ROOT / "k8s/omnigent-server"
    values = chart / "lint_values.yaml"
    for override in ("cnab.images.main.digest=", "replicas=2"):
        result = subprocess.run(
            [
                "helm",
                "template",
                "omnigent-server",
                str(chart),
                "-f",
                str(values),
                "--set",
                override,
            ],
            capture_output=True,
            text=True,
            check=False,
        )
        assert result.returncode != 0
