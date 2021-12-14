import os
from urllib.parse import urlparse
from hashlib import md5

from fsspec.asyn import AsyncFileSystem, sync, sync_wrapper
from fsspec.implementations.http import HTTPFileSystem
import aiohttp

import logging

logger = logging.getLogger("swiftspec")

MAX_RETRIES = 2


class SWIFTRef:
    def __init__(self, ref):
        parts = urlparse(ref)
        self.host = parts.netloc
        if parts.scheme == "swift":
            split_parts = parts.path.split("/", 3)[1:]
        elif parts.scheme == "https":
            split_parts = parts.path.split("/", 4)[2:]
        else:
            raise ValueError("unknown SWIFT url scheme '{}' in '{}'".format(parts.scheme, ref))
        split_parts += [None, None]
        self.account = split_parts[0]
        self.container = split_parts[1] or None
        self.object = split_parts[2] or None
            
    @property
    def http_url(self):
        if self.object:
            return f"https://{self.host}/v1/{self.account}/{self.container}/{self.object}"
        elif self.container:
            return f"https://{self.host}/v1/{self.account}/{self.container}"
        else:
            return f"https://{self.host}/v1/{self.account}"

def swift_res_to_info(prefix, res):
    if "subdir" in res:
        name = res["subdir"]
        assert name.endswith("/")
        return {"name": prefix + name[:-1], "size": None, "type": "directory"}
    else:
        name = res["name"]
        extra_attrs = {k: v for k, v in res.items() if k not in {"bytes", "name", "size", "type"}}
        return {"name": prefix + name,
                "size": res["bytes"],
                "type": "file",
                **extra_attrs}


class SWIFTFileSystem(HTTPFileSystem):
    protocol = "swift"
    sep = "/"

    def __init__(self, auth=None, block_size=None, asynchronous=False, loop=None, client_kwargs=None, **storage_options):
        self.auth = (auth or []) + self.get_tokens_from_env()
        super().__init__(simple_links=False,
                         block_size=block_size,
                         same_scheme=True,
                         asynchronous=asynchronous,
                         loop=loop,
                         client_kwargs=client_kwargs,
                         **storage_options)

    def get_tokens_from_env(self):
        token = os.environ.get("OS_AUTH_TOKEN")
        url = os.environ.get("OS_STORAGE_URL")
        if token and url:
            return [{"token": token, "url": url}]
        else:
            return []

    def headers_for_url(self, url):
        headers = {}
        for auth in self.auth:
            if url.startswith(auth["url"]):
                headers["X-Auth-Token"] = auth["token"]
                break
        return headers

    async def _ls(self, path, detail=True, **kwargs):
        ref = SWIFTRef(path)
        session = await self.set_session()
        if not ref.container:
            params = {
                "format": "json",
            }
            url = f"https://{ref.host}/v1/{ref.account}"
            async with session.get(url, params=params, headers=self.headers_for_url(url)) as res:
                res.raise_for_status()
                resdata = await res.json()
            info = [{"name": f"swift://{ref.host}/{ref.account}/" + e["name"], "size": e["bytes"], "type": "directory"} for e in await res.json()]
        else:
            if ref.object:
                prefix = ref.object
                if not prefix.endswith("/"):
                    prefix += "/"
            else:
                prefix = ""
            params = {
                "format": "json",
                "delimiter": "/",
                "prefix": prefix,
            }
            url = f"https://{ref.host}/v1/{ref.account}/{ref.container}"
            async with session.get(url, params=params, headers=self.headers_for_url(url)) as res:
                res.raise_for_status()
                resdata = await res.json()
            info = [swift_res_to_info(f"swift://{ref.host}/{ref.account}/{ref.container}/", entry) for entry in await res.json()]
        if detail:
            return info
        else:
            return [e["name"] for e in info]

    ls = sync_wrapper(_ls)

    async def _cat_file(self, path, start=None, end=None, **kwargs):
        ref = SWIFTRef(path)
        headers = self.headers_for_url(ref.http_url)
        return await super()._cat_file(ref.http_url, start=start, end=end, headers=headers, **kwargs)

    async def _get_file(self, rpath, lpath, **kwargs):
        ref = SWIFTRef(path)
        headers = self.headers_for_url(ref.http_url)
        return await super()._get_file(ref.http_url, lpath=lpath, headers=headers, **kwargs)

    async def _pipe_file(self, path, data, chunksize=50 * 2 ** 20, **kwargs):
        print(f"PIPE {path}")
        ref = SWIFTRef(path)
        size = len(data)
        if not ref.object:
            raise ValueError("given path is not an object")
        if size > 5 * 2 ** 30:  # 5 GB is maximum PUT size for swift
            raise NotImplementedError("large objects are not implemented")
            # see https://docs.openstack.org/swift/latest/api/large_objects.html#static-large-objects
            # and https://docs.openstack.org/api-ref/object-store/?expanded=show-object-metadata-detail,create-or-replace-object-detail#create-or-replace-object

        url = ref.http_url
        headers = self.headers_for_url(url)
        headers["Content-Length"] = str(size)
        headers["ETag"] = md5(data).hexdigest()  # in swift, ETag is alwas the MD5sum and will be used by the server to verify the upload

        session = await self.set_session()
        async with session.put(url, data=data, headers=headers) as res:
            res.raise_for_status()

    async def _put_file(self, lpath, rpath, **kwargs):
        ref = SWIFTRef(path)
        headers = self.headers_for_url(ref.http_url)
        kwargs = {**kwargs, "method": "put"}
        return await super()._put_file(lpath=lpath, rpath=ref.http_url, headers=headers, **kwargs)

    async def _exists(self, path, **kwargs):
        ref = SWIFTRef(path)
        headers = self.headers_for_url(ref.http_url)
        kwargs = {**kwargs, "headers": {**kwargs.get("headers", {}), **headers}}
        #TODO: maybe better API call
        return await super()._exists(ref.http_url, **kwargs)

    async def _isfile(self, path, **kwargs):
        ref = SWIFTRef(path)
        headers = self.headers_for_url(ref.http_url)
        kwargs = {**kwargs, "headers": {**kwargs.get("headers", {}), **headers}}
        #TODO: maybe better API call
        return await super()._isfile(ref.http_url, **kwargs)

    def _open(self, path, *args, **kwargs):
        ref = SWIFTRef(path)
        headers = self.headers_for_url(ref.http_url)
        kwargs = {**kwargs, "headers": {**kwargs.get("headers", {}), **headers}}
        return super()._open(ref.http_url, *args, **kwargs)

    def ukey(self, path):
        return tokenize(url, self.kwargs, self.info(path)["ETag"])

    async def _info(self, path, **kwargs):
        headers = self.headers_for_url(ref.http_url)
        kwargs = {**kwargs, "headers": {**kwargs.get("headers", {}), **headers}}
        return await super()._info(ref.http_url, **kwargs)
