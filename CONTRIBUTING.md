# Contributing

Use Windows x64 and standard Python 3.13. Keep development environments separate from
operator environments and active import work directories.

```powershell
py -3.13 -m venv .venv-dev
$DevPython = (Resolve-Path '.\.venv-dev\Scripts\python.exe').Path
& $DevPython -m pip install -e '.[dev,xml]'
if ($LASTEXITCODE -ne 0) { throw 'Development installation failed.' }
& $DevPython -m pytest tests --ignore=tests/integration -q
if ($LASTEXITCODE -ne 0) { throw 'Tests failed.' }
```

Integration tests are opt-in and require disposable databases. Read [testing](docs/testing.md)
before enabling them. Never use private exports or production databases as fixtures.
Use small synthetic fixtures with independently specified expected values. Preserve malformed
bytes and encoding fixtures unchanged; `.gitattributes` disables newline conversion for them.

Read [code structure and recovery boundaries](docs/development.md) before changing the pipeline.
Changes to parsing, naming, publication or recovery need corresponding regression tests.
Do not weaken receipt checks, original-byte preservation or memory bounds to make a test pass.
Update the relevant configuration reference and operator example when behavior changes.
Generate the reference with `python -X utf8 scripts/generate_configuration_reference.py`.

## Preparing public source

```powershell
& $DevPython scripts/package_source.py --output releases/db-ingest-source.zip
if ($LASTEXITCODE -ne 0) { throw 'Review the source-package checks.' }
```

The source packager checks Git-visible files against a public directory/file allowlist,
rejects private artifacts and common credential patterns, checks local Markdown links, and
writes a source ZIP without Git history, environments, test output or release binaries.
It refuses to overwrite an existing ZIP. Review `git status --short` and the resulting ZIP
before publishing. The checks reduce accidental disclosure; they do not replace review.

For a new public repository, extract the reviewed source ZIP into a fresh folder and
initialize Git there. Publish the contents of its `db-ingest` folder. Do not upload the
entire development workspace or copy its `.git` directory: ignored private evidence and
local environments may still exist on disk.

Commit source, tests, documentation and synthetic examples. Attach operator/offline ZIPs as
GitHub Release assets rather than committing them. Keep native Python/ODBC installers separate.
Build application wheels with `python -m pip wheel --no-deps . --wheel-dir dist` in a build
environment using the pinned build requirements. Test each release in a fresh environment.
