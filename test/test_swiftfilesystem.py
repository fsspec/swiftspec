import json
import re
from contextlib import asynccontextmanager
from hashlib import md5

from swiftspec import SWIFTFileSystem


class MockResponse:
    def __init__(self, status, content):
        self.status = status
        self.content = content

    async def json(self):
        return json.loads(self.content)

    def raise_for_status(self):
        if self.status != 200:
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
    def __init__(self, router, data):
        self.router = router
        self.data = data

    @asynccontextmanager
    async def get(self, url, params=None, headers=None):
        protocol, _, host, path = url.split("/", 3)
        assert protocol == "https:"
        params = params or {}
        headers = headers or {}
        yield self.router("/" + path, "get", data=self.data)

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
    def __init__(self, data):
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
            for k, v in self.data[account].items()
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
            for k, v in self.data[account][container].items()
        ]
        return MockResponse(200, json.dumps(objects))


async def get_client(**kwargs):
    router = Router(
        [
            ("^/v1/(?P<account>[^/]+)$", AccountHandler),
            ("^/v1/(?P<account>[^/]+)/(?P<container>[^/]+)$", ContainerHandler),
        ]
    )
    data = create_mock_data()
    return MockClient(router, data)


def test_ls_account():
    fs = SWIFTFileSystem(get_client=get_client)
    res = fs.ls("swift://server/a1")
    assert len(res) == 1
    assert res[0]["name"] == "swift://server/a1/c1"
    assert res[0]["type"] == "directory"


def test_ls_container():
    fs = SWIFTFileSystem(get_client=get_client)
    res = fs.ls("swift://server/a1/c1")
    assert len(res) == 1
    assert res[0]["name"] == "swift://server/a1/c1/hello"
    assert res[0]["type"] == "file"
    assert res[0]["size"] == len(b"Hello World")
