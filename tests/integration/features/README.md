# Slurm charms BDD integration tests

This directory contains the BDD (Behavior-Driven Development) integration test
suite for the Slurm charms, migrated from the legacy `jubilant`-based tests to
[`pytest-jubilant-bdd`](https://github.com/canonical/pytest-jubilant-bdd) using
[`gherkinator`](https://github.com/canonical/gherkinator) for test plan
management.

## What's here

```
tests/integration/
├── features/                                  # gherkinator-controlled
│   ├── test-plan.yaml                         # SOURCE OF TRUTH (YAML)
│   ├── slurm_cluster_deployment.feature       # GENERATED
│   ├── slurm_services_health.feature          # GENERATED
│   ├── slurm_node_operations.feature          # GENERATED
│   ├── slurm_key_rotation.feature             # GENERATED
│   ├── slurm_job_submission.feature           # GENERATED
│   ├── slurm_mail_notifications.feature       # GENERATED
│   ├── slurm_oci_runtime.feature              # GENERATED
│   ├── slurm_influxdb_accounting.feature      # GENERATED
│   └── slurmctld_high_availability.feature    # GENERATED
├── bdd_utils.py                               # Shared fixtures + helpers
├── conftest.py                                # Pytest config + shared fixtures
├── constants.py                               # Test constants
├── test_cluster_deployment_bdd.py             # Step definitions (order 1)
├── test_services_health_bdd.py                # Step definitions (order 2)
├── test_node_operations_bdd.py                # Step definitions (order 6)
├── test_key_rotation_bdd.py                   # Step definitions (order 10)
├── test_job_submission_bdd.py                 # Step definitions (order 12)
├── test_mail_bdd.py                           # Step definitions (order 14)
├── test_oci_runtime_bdd.py                    # Step definitions (order 13)
├── test_influxdb_bdd.py                       # Step definitions (skipped)
└── test_ha_bdd.py                             # Step definitions (order 19, HA-gated)
```

## Key principles

- **`features/test-plan.yaml` is the single source of truth.** Edit it, then
  validate and regenerate `.feature` files. Never hand-edit `.feature` files —
  they are overwritten on every `gherkinator generate`.
- **No `juju` fixture in BDD modules.** The `pytest-jubilant-bdd` plugin
  provides a session-scoped `context` fixture that replaces the hand-rolled
  `juju` fixture. Step definitions access the Juju harness via
  `context.get_juju()`.
- **No step handler imports.** The plugin auto-registers its `@given`/`@when`/
  `@then` handlers via the `pytest11` entry point. Only `Context`,
  `assertions`, `flexible`, `make_dict`, and `make_list` are importable from
  `pytest_jubilant_bdd`, and only when authoring custom steps.
- **`context.wait()` replaces `tenacity`.** Custom Then steps poll via
  `context.wait(ready=...)` instead of `tenacity.Retrying`.
- **Cross-file ordering preserved.** Each `test_*_bdd.py` module sets
  `pytestmark = pytest.mark.order(N)` matching the legacy test's order.

## Prerequisites

BDD dependencies are included in the `dev` extras in `pyproject.toml`. No
separate requirements file is needed.

```bash
uv sync --extra dev
```

## Workflow

### Edit a scenario

1. Edit `features/test-plan.yaml`.
2. Validate: `gherkinator validate tests/integration/features/test-plan.yaml`
3. Regenerate: `gherkinator generate --format gh tests/integration/features/test-plan.yaml --output-dir tests/integration/features`
4. Run: `pytest tests/integration/ -v -k bdd`

### Install gherkinator

```bash
# Snap (recommended)
sudo snap install gherkinator --classic

# From source
git clone https://github.com/canonical/gherkinator.git
cd gherkinator && go build -o ~/.local/bin/gherkinator ./cmd/gherkinator
```

## Feature overview

| Feature | Type | Risk | Status | Order | Step definitions |
|---|---|---|---|---|---|
| Slurm cluster deployment | functional | edge | implemented | 1 | `test_cluster_deployment_bdd.py` |
| Slurm services health | functional | edge | implemented | 2 | `test_services_health_bdd.py` |
| Slurm node operations | functional | edge | implemented | 6 | `test_node_operations_bdd.py` |
| Slurm key rotation | functional | edge | implemented | 10 | `test_key_rotation_bdd.py` |
| Slurm job submission | functional | edge | implemented | 12 | `test_job_submission_bdd.py` |
| Slurm mail notifications | functional | edge | implemented | 14 | `test_mail_bdd.py` |
| Slurm OCI runtime | functional | edge | implemented | 13 | `test_oci_runtime_bdd.py` |
| Slurm InfluxDB accounting | functional | edge | planned | 12 | `test_influxdb_bdd.py` (skipped) |
| Slurmctld high availability | reliability | edge | planned | 19 | `test_ha_bdd.py` (HA-gated) |

Each `TestPlan` document in `test-plan.yaml` has a `status` field. Features
that are green are marked `status: implemented`; features that are not yet
green remain `status: planned`. Run `gherkinator generate` after changing
the status so the `.feature` file carries the corresponding tag.

## Custom steps

Custom steps are defined in the `test_*_bdd.py` modules for behavior the
framework doesn't cover:

- **Cluster deploy** — charm names differ from app names (e.g. `sackd` →
  `login`); `slurmctld` needs constraints and config.
- **scontrol JSON** — node state/weight/reason assertions via
  `scontrol --json show node`.
- **Port/metrics checks** — `lsof` and `curl` output assertions.
- **Key rotation** — `/etc/slurm/slurm.jwks` and `/etc/slurm/jwt_hs256.key`
  inspection, REST API token probing.
- **GPU mock** — `/sys`/`/proc`/`/dev` overlay mounts for NVIDIA
  auto-detection.
- **Mail capture** — local SMTP server (`aiosmtpd`) for notification emails.
- **HA** — `scontrol ping --json` controller discovery, machine power cycling
  via `lxc`, `systemctl status` service role assertions.
