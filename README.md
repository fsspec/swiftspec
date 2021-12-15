# swiftspec

```python
import fsspec

with fsspec.open("swift://server/account/container/object.txt", "r") as f:
    print(f.read())
```

## Authentication

`swiftspec` uses the environment variables `OS_STORAGE_URL` and `OS_AUTH_TOKEN` for authentication if available. To create these variables, you can use the `swift auth` command from the [python-swiftclient](https://docs.openstack.org/python-swiftclient/latest/cli/index.html).

## Develop

### Code Formatting

swiftspec uses [Black](https://black.readthedocs.io/en/stable) to ensure
a consistent code format throughout the project.
Run ``black .`` from the root of the swiftspec repository to
auto-format your code. Additionally, many editors have plugins that will apply
``black`` as you edit files.


Optionally, you may wish to setup [pre-commit hooks](https://pre-commit.com) to
automatically run ``black`` when you make a git commit.
Run ``pre-commit install --install-hooks`` from the root of the
swiftspec repository to setup pre-commit hooks. ``black`` will now be run
before you commit, reformatting any changed files. You can format without
committing via ``pre-commit run`` or skip these checks with ``git commit
--no-verify``.
