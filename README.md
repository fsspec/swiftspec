# swiftspec

```python
import fsspec

with fsspec.open("swift://server/account/container/object.txt", "r") as f:
    print(f.read())
```

## Authentication

`swiftspec` uses the environment variables `OS_STORAGE_URL` and `OS_AUTH_TOKEN` for authentication if available. To create these variables, you can use the `swift auth` command from the [python-swiftclient](https://docs.openstack.org/python-swiftclient/latest/cli/index.html).
