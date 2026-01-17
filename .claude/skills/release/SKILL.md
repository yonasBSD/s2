---
name: release
description: Release a new version of the project with semver validation, Cargo.toml updates, and crates.io publishing
---

# /release

Release a new version of the project.

## Usage

```
/release [version]
```

If version is not provided, prompt the user for it.

## Steps

1. **Verify prerequisites**
   - Confirm on `main` branch
   - Confirm working directory is clean (`git status`)
   - Pull latest (`git pull`)

2. **Validate version**
   - Version should follow semver (X.Y.Z)
   - Confirm it's greater than current version in Cargo.toml

3. **Update Cargo.toml**
   - Edit root `Cargo.toml` to update version in all 3 places:
     - `workspace.package.version`
     - `workspace.dependencies.s2-api`
     - `workspace.dependencies.s2-common`

4. **Update lockfile**
   ```bash
   cargo generate-lockfile
   ```

5. **Commit and push**
   ```bash
   git add Cargo.toml Cargo.lock
   git commit -m "release: X.Y.Z"
   git push
   ```

6. **Publish to crates.io**
   ```bash
   just publish
   ```
   Wait for this to complete successfully before proceeding.

7. **Tag and trigger release**
   ```bash
   just tag X.Y.Z
   ```

## Notes

- If any step fails, stop and report the error
- The `just tag` command triggers GitHub Actions to build Docker images and create the GitHub release
