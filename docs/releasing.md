# Releasing pydirs

Releases are published to PyPI automatically when a GitHub Release is
published. The distribution name is `pydirs`.

## One-time setup

### 1. Create a PyPI API token

1. Sign in to <https://pypi.org>.
2. Open **Account settings → API tokens** and choose **Add API token**.
3. For the first upload of a new project, the token must be scoped to
   **Entire account**. After `pydirs` exists on PyPI, create a new token
   scoped to that project only and use it instead.
4. Copy the token. It starts with `pypi-` and is shown only once.

Before the first release, check that the name `pydirs` is still free on
PyPI (<https://pypi.org/project/pydirs/>).

### 2. Add the repository secret

1. On GitHub, open the repository and go to **Settings → Secrets and variables
   → Actions**.
2. Choose **New repository secret**.
3. Name: `PYPI_API_TOKEN`. Value: the `pypi-...` token from step 1.
4. Save.

The publish job reads this secret as `${{ secrets.PYPI_API_TOKEN }}`. The name
must match exactly.

## Cutting a release

### 1. Bump the version

Update only the `version` field in `Cargo.toml`, following semantic versioning:

```toml
[package]
version = "0.2.0"   # was 0.1.0
```

`pyproject.toml` reads its version from `Cargo.toml` (`dynamic = ["version"]`),
so there is no second version to bump.

Also update `CHANGELOG.md`: move items from **Unreleased** into a new section
for the new version, with today's date.

Commit and merge the change to `main` through a pull request. CI must pass.

### 2. Create the release

1. On GitHub, go to **Releases → Draft a new release**.
2. **Choose a tag**: type `v0.2.0` (the leading `v` is optional, but the rest
   must match `Cargo.toml` exactly), and select **Create new tag on publish**.
3. Target: `main`.
4. Title: `v0.2.0`. Paste the matching CHANGELOG section into the description.
5. Click **Publish release**.

Publishing the release is what starts the workflow. Creating a tag by itself,
or pushing a tag, does **not** start it.

## What the workflow does

The workflow is `.github/workflows/release.yml`. It runs only on
`release: published` events.

| Job | Purpose |
|---|---|
| `check-version` | Strips a leading `v` from the tag and compares it to `version` in `Cargo.toml`. Fails the run on mismatch. |
| `build-wheels` | Builds wheels with `PyO3/maturin-action` for linux x86_64 and aarch64 (manylinux), linux x86_64 and aarch64 (musllinux), macOS x86_64 and arm64, and windows x64. One abi3 wheel per target covers Python 3.9 to 3.14. |
| `build-sdist` | Builds the source distribution. |
| `smoke-test` | Installs the built wheel on Python 3.9 and 3.14 (Ubuntu), imports `pydirs`, and runs a `set`/`get` round trip. |
| `publish` | Runs after all jobs above succeed. Downloads every artifact into `dist/` and uploads it with `pypa/gh-action-pypi-publish`. |

`skip-existing: true` is set, so re-running the workflow for a version already
on PyPI does not fail. It also means a partially failed upload can be retried
without bumping the version.

Watch progress under **Actions → Release** in the GitHub repository.

## Troubleshooting

**`check-version` fails with "does not match Cargo.toml version".**
The tag and `Cargo.toml` disagree. Delete the release and tag, bump
`Cargo.toml` on `main` so it matches the intended tag, and publish a new
release. Do not reuse a version that was already uploaded to PyPI, because PyPI
never allows re-uploading a file with the same name.

**`publish` fails with `403 Forbidden` or "Invalid or non-existent authentication".**
The `PYPI_API_TOKEN` secret is missing, misspelled, expired or revoked. Check
the secret name, then create a new token on PyPI and update the secret.

**`publish` says the file already exists.**
With `skip-existing: true` this should not fail. If it does, the file for that
exact version and platform is already on PyPI with different contents. Bump the
version.

**A wheel is missing for one platform.**
Look at the `build-wheels` matrix entry for that platform. Maturin errors are
usually in the build log of that job. Re-run the failed job from the Actions
page once the cause is fixed. Re-running the whole release does not rebuild
jobs that already succeeded in the same run.

**`smoke-test` fails on import.**
The wheel built, but the native module does not load. Check the `Install wheel`
step first, to confirm which wheel was picked up. Then check that the module
name in `pyproject.toml` (`pydirs._pydirs`) matches what `src/lib.rs`
exports.

**Installing from PyPI fails on an uncommon platform.**
pip falls back to the source distribution and needs a Rust toolchain. Add a
wheel target to `release.yml` if the platform is common enough to support.

**I published by mistake.**
PyPI does not allow deleting and re-uploading a version. Yank the release on
PyPI (it stays installable only by exact pin), then publish a fixed patch
version.
