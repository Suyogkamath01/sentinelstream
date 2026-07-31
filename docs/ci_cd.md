# CI/CD guide

GitHub Actions are split by responsibility:

| Workflow | Trigger | Scope |
| --- | --- | --- |
| `ci.yml` | Pull request | Ruff, mypy, fast tests, coverage artifact |
| `main-validation.yml` | Main, manual, weekly | Full checks, docs, benchmark smoke, Compose syntax |
| `security.yml` | Pull request, main, manual, weekly | Bandit, pip-audit, Gitleaks, Trivy, dependency review |
| `codeql.yml` | Pull request, main, weekly | CodeQL Python analysis |
| `docker.yml` | Pull request, main, manual | Compose validation, API/dashboard/Spark builds, Trivy image scan |
| `docs.yml` | Pull request, main, manual | Required files, local links, YAML validation |
| `release.yml` | Manual | Revalidate; optionally publish tagged images to GHCR |

The workflows use least-privilege permissions and do not publish images unless
the release input explicitly enables it. Action versions are pinned to stable
major versions; production governance may pin them to commit SHAs.

Recommended repository settings are required checks for PR validation, docs,
security, and Docker workflows; at least one review; resolved conversations;
blocked direct pushes to `main`; and protected release tags. These are
recommendations only—the repository settings are not changed by this project.

Run the same checks locally with:

```bash
make install
make check
make docs-validate
docker compose config
```
