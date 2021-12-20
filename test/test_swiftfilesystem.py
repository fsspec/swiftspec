import json
import re
from contextlib import asynccontextmanager
from hashlib import md5

import pytest

from swiftspec import SWIFTFileSystem

RANGE_RE = re.compile("bytes=([0-9]*)-([0-9]*)")


def range_to_slice(range_header):
    m = RANGE_RE.match(range_header)
    if not m:
        raise ValueError(f"could not parse range: '{range_header}'")
    start = m.group(1)
    end = m.group(2)
    if start:
        start = int(start)
        if end:
            end = int(end)
            return slice(start, end)
        else:
            return slice(start, None)
    else:
        if end:
            end = int(end)
            return slice(-end, None)
        else:
            raise ValueError(f"invalid range: '{range_header}'")


class MockResponse:
    def __init__(self, status, content, headers=None):
        self.status = status
        self.content = content
        self.headers = headers or {}

    async def read(self):
        return self.content

    async def json(self):
        return json.loads(self.content)

    def raise_for_status(self):
        if self.status // 100 != 2:
            raise RuntimeError(f"status {self.status}")


class Router:
    def __init__(self, routes):
        self.routes = [(re.compile(route), handler) for route, handler in routes]

    def __call__(self, url, method, **kwargs):
        for pattern, handler in self.routes:
            m = pattern.match(url)
            if m:
                return getattr(handler(**kwargs), method)(**m.groupdict())
        else:
            return MockResponse(404, "not found")


class MockClient:
    def __init__(self, router, store):
        self.router = router
        self.store = store

    @asynccontextmanager
    async def _method(self, method, url, params=None, headers=None, data=None):
        protocol, _, host, path = url.split("/", 3)
        assert protocol == "https:"
        params = params or {}
        headers = headers or {}
        yield self.router(
            "/" + path, method, store=self.store, headers=headers, data=data
        )

    def get(self, url, params=None, headers=None):
        return self._method("get", url, params, headers)

    def head(self, url, params=None, headers=None):
        return self._method("head", url, params, headers)

    def put(self, url, params=None, headers=None, data=None):
        return self._method("put", url, params, headers, data)

    def delete(self, url, params=None, headers=None, data=None):
        return self._method("delete", url, params, headers, data)

    async def close(self):
        pass


def create_mock_data():
    return {
        "a1": {
            "c1": {
                "hello": b"Hello World",
            },
        }
    }


class SWIFTHandler:
    def __init__(self, store, headers, data):
        self.store = store
        self.headers = headers
        self.data = data


class AccountHandler(SWIFTHandler):
    def get(self, account):
        containers = [
            {
                "count": len(v),
                "bytes": None,
                "name": k,
                "last_modified": "2016-04-29T16:23:50.460230",
            }
            for k, v in self.store[account].items()
        ]
        return MockResponse(200, json.dumps(containers))


class ContainerHandler(SWIFTHandler):
    def get(self, account, container):
        objects = [
            {
                "hash": md5(v).hexdigest(),
                "last_modified": "2014-01-15T16:41:49.390270",
                "bytes": len(v),
                "name": k,
                "content_type": "application/octet-stream",
            }
            for k, v in self.store[account][container].items()
        ]
        return MockResponse(200, json.dumps(objects))


class ObjectHandler(SWIFTHandler):
    def get(self, account, container, obj):
        try:
            data = self.store[account][container][obj]
        except KeyError:
            return MockResponse(404, "not found")

        if "Range" in self.headers:
            data = data[range_to_slice(self.headers["Range"])]

        headers = {
            "Content-Length": str(len(data)),
            "Etag": md5(data).hexdigest(),
        }
        return MockResponse(200, data, headers=headers)

    def head(self, account, container, obj):
        res = self.get(account, container, obj)
        return MockResponse(res.status, b"", headers=res.headers)

    def put(self, account, container, obj):
        if "Content-Length" not in self.headers:
            return MockResponse(411, "length required")
        length = int(self.headers["Content-Length"])
        assert len(self.data) == length
        if "Etag" in self.headers:
            assert self.headers["Etag"] == md5(self.data).hexdigest()
        self.store[account][container][obj] = self.data
        return MockResponse(201, "created")

    def delete(self, account, container, obj):
        if obj in self.store[account][container]:
            del self.store[account][container][obj]
        return MockResponse(204, "no content")


async def get_client(**kwargs):
    router = Router(
        [
            ("^/v1/(?P<account>[^/]+)$", AccountHandler),
            ("^/v1/(?P<account>[^/]+)/(?P<container>[^/]+)$", ContainerHandler),
            (
                "^/v1/(?P<account>[^/]+)/(?P<container>[^/]+)/(?P<obj>.+)$",
                ObjectHandler,
            ),
        ]
    )
    data = create_mock_data()
    return MockClient(router, data)


@pytest.fixture
def fs():
    yield SWIFTFileSystem(get_client=get_client)
    SWIFTFileSystem.clear_instance_cache()


def test_ls_account(fs):
    res = fs.ls("swift://server/a1")
    assert len(res) == 1
    assert res[0]["name"] == "swift://server/a1/c1"
    assert res[0]["type"] == "directory"


def test_ls_container(fs):
    res = fs.ls("swift://server/a1/c1")
    assert len(res) == 1
    assert res[0]["name"] == "swift://server/a1/c1/hello"
    assert res[0]["type"] == "file"
    assert res[0]["size"] == len(b"Hello World")


def test_cat(fs):
    assert fs.cat("swift://server/a1/c1/hello") == b"Hello World"


def test_cat_partial(fs):
    assert fs.cat("swift://server/a1/c1/hello", start=3, end=5) == b"lo"


EXIST_CASES = [
    ("swift://server/a1/c1/hello", True, False),
    ("swift://server/a1/c1/not_there", False, False),
    ("swift://server/a1/c1", False, True),
    ("swift://server/a1/c1/", False, True),
]


@pytest.mark.parametrize("path,is_file,is_dir", EXIST_CASES)
def test_exists(path, is_file, is_dir, fs):
    assert fs.exists(path) == (is_file or is_dir)
    assert fs.isfile(path) == is_file
    assert fs.isdir(path) == is_dir


def test_pipe(fs):
    fs.pipe("swift://server/a1/c1/foo", b"bar")
    assert fs._session.store["a1"]["c1"]["foo"] == b"bar"


def test_rm(fs):
    fs.rm("swift://server/a1/c1/hello")
    assert "hello" not in fs._session.store["a1"]["c1"]


def test_open_read(fs):
    with fs.open("swift://server/a1/c1/hello", "r") as f:
        assert f.read() == "Hello World"

    with fs.open("swift://server/a1/c1/hello", "rb") as f:
        assert f.read() == b"Hello World"


def test_open_write(fs):
    with fs.open("swift://server/a1/c1/w", "wb") as f:
        f.write(b"write test")
    assert fs._session.store["a1"]["c1"]["w"] == b"write test"
