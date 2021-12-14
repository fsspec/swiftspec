# swiftspec

```python
import fsspec

with fsspec.open("swift://server/account/container/object.txt", "r") as f:
    print(f.read())
```
