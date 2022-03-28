# swiftspec

```python
import fsspec

with fsspec.open("swift://server/account/container/object.txt", "r") as f:
    print(f.read())
```

## Authentication

`swiftspec` uses the environment variables `OS_STORAGE_URL` and `OS_AUTH_TOKEN` for authentication if available. To create these variables, you can use the `swift auth` command from the [python-swiftclient](https://docs.openstack.org/python-swiftclient/latest/cli/index.html).

## fault tolerance / automatic retry

Sometimes reading or writing from / to a swift storage might fail occasionally. If many objects are accessed, occasional failures can be extremely annoying and could be fixed relatively easily by retrying the request. Fortunately the `aiohttp_retry` package can help out in these situations. `aiohttp_retry` provides a wrapper around an `aiohttp` Client, which will automatically retry requests based on some user-provided rules. You can inject this client into the `swiftspec` filesystem using the `get_client` argument. First you'll have to define an async `get_client` function, which configures the `RetryClient` according to your preferences, e.g.:

```python
async def get_client(**kwargs):
    import aiohttp
    import aiohttp_retry
    retry_options = aiohttp_retry.ExponentialRetry(
            attempts=3,
            exceptions={OSError, aiohttp.ServerDisconnectedError})
    retry_client = aiohttp_retry.RetryClient(raise_for_status=False, retry_options=retry_options)
    return retry_client
```

afterwards, you can use this function like:

```python
with fsspec.open("swift://server/account/container/object.txt", "r", get_client=get_client) as f:
    print(f.read())
```

or:


```python
import xarray as xr
ds = xr.Dataset(...)
ds.to_zarr("swift://server/account/container/object.zarr", storage_options={"get_client": get_client})
```

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
