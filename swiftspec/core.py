import logging
import os
import weakref
from hashlib import md5
from urllib.parse import urlparse

import aiohttp
from fsspec.asyn import AsyncFileSystem, sync, sync_wrapper
from fsspec.exceptions import FSTimeoutError
from fsspec.spec import AbstractBufferedFile
from fsspec.utils import tokenize

logger = logging.getLogger("swiftspec")

MAX_RETRIES = 2


async def get_client(**kwargs):
    return aiohttp.ClientSession(**kwargs)


class SWIFTRef:
    def __init__(self, ref):
        parts = urlparse(ref)
        self.host = parts.netloc
        if parts.scheme == "swift":
            split_parts = parts.path.split("/", 3)[1:]
        elif parts.scheme == "https":
            split_parts = parts.path.split("/", 4)[2:]
        else:
            raise ValueError(
                "unknown SWIFT url scheme '{}' in '{}'".format(parts.scheme, ref)
            )
        split_parts += [None, None]
        self.account = split_parts[0]
        self.container = split_parts[1] or None
        self.object = split_parts[2] or None

    @property
    def http_url(self):
        if self.object:
            return (
                f"https://{self.host}/v1/{self.account}/{self.container}/{self.object}"
            )
        elif self.container:
            return f"https://{self.host}/v1/{self.account}/{self.container}"
        else:
            return f"https://{self.host}/v1/{self.account}"

    @property
    def swift_url(self):
        if self.object:
            return f"swift://{self.host}/{self.account}/{self.container}/{self.object}"
        elif self.container:
            return f"swift://{self.host}/{self.account}/{self.container}"
        else:
            return f"swift://{self.host}/{self.account}"


def swift_res_to_info(prefix, res):
    if "subdir" in res:
        name = res["subdir"]
        assert name.endswith("/")
        return {"name": prefix + name[:-1], "size": None, "type": "directory"}
    else:
        name = res["name"]
        extra_attrs = {
            k: v for k, v in res.items() if k not in {"bytes", "name", "size", "type"}
        }
        return {
            "name": prefix + name,
            "size": res["bytes"],
            "type": "file",
            **extra_attrs,
        }


class SWIFTFile(AbstractBufferedFile):
    def _upload_chunk(self, final=False):
        if not final:
            raise NotImplementedError(
                "currently only single chunk uploads are implemented"
            )
        self.buffer.seek(0)
        self.fs.pipe_file(self.path, self.buffer.read())
        return True

    def _fetch_range(self, start, end):
        """Get the specified set of bytes from remote"""
        return self.fs.cat_file(self.path, start, end)


class SWIFTFileSystem(AsyncFileSystem):
    protocol = "swift"
    sep = "/"

    def __init__(
        self,
        auth=None,
        block_size=None,
        asynchronous=False,
        loop=None,
        get_client=get_client,
        client_kwargs=None,
        verify_uploads=True,
        **storage_options,
    ):
        self.auth = (auth or []) + self.get_tokens_from_env()
        super().__init__(
            block_size=block_size,
            asynchronous=asynchronous,
            loop=loop,
            **storage_options,
        )

        self.get_client = get_client
        self.client_kwargs = client_kwargs or {}
        self.verify_uploads = verify_uploads
        self._session = None

    @staticmethod
    def close_session(loop, session):
        if loop is not None and loop.is_running():
            try:
                sync(loop, session.close, timeout=0.1)
                return
            except (TimeoutError, FSTimeoutError):
                pass
        connector = getattr(session, "_connector", None)
        if connector is not None:
            # close after loop is dead
            connector._close()

    async def set_session(self):
        if self._session is None:
            self._session = await self.get_client(loop=self.loop, **self.client_kwargs)
            if not self.asynchronous:
                weakref.finalize(self, self.close_session, self.loop, self._session)
        return self._session

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

    @classmethod
    def _strip_protocol(cls, path):
        """For SWIFT, we always want to keep the full URL"""
        return path

    async def _ls(self, path, detail=True, **kwargs):
        ref = SWIFTRef(path)
        session = await self.set_session()
        if not ref.container:
            params = {
                "format": "json",
            }
            url = f"https://{ref.host}/v1/{ref.account}"
            async with session.get(
                url, params=params, headers=self.headers_for_url(url)
            ) as res:
                res.raise_for_status()
                resdata = await res.json()
            info = [
                {
                    "name": f"swift://{ref.host}/{ref.account}/" + e["name"],
                    "size": e["bytes"],
                    "type": "directory",
                }
                for e in await res.json()
            ]
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
            async with session.get(
                url, params=params, headers=self.headers_for_url(url)
            ) as res:
                res.raise_for_status()
                resdata = await res.json()
            info = [
                swift_res_to_info(
                    f"swift://{ref.host}/{ref.account}/{ref.container}/", entry
                )
                for entry in resdata
            ]
        if detail:
            return info
        else:
            return [e["name"] for e in info]

    ls = sync_wrapper(_ls)

    async def _cat_file(self, path, start=None, end=None, **kwargs):
        ref = SWIFTRef(path)
        headers = self.headers_for_url(ref.http_url)
        if start is not None:
            assert start >= 0
            if end is not None:
                assert end >= 0
                headers["Range"] = f"bytes={start}-{end}"
            else:
                headers["Range"] = f"bytes={start}-"
        else:
            if end is not None:
                assert end >= 0
                headers["Range"] = f"bytes=0-{end}"

        session = await self.set_session()
        async with session.get(ref.http_url, headers=headers) as res:
            res.raise_for_status()
            return await res.read()

    async def _pipe_file(self, path, data, chunksize=50 * 2 ** 20, **kwargs):
        ref = SWIFTRef(path)
        size = len(data)
        if not ref.object:
            raise ValueError("given path is not an object")
        if size > 5 * 2 ** 30:  # 5 GB is maximum PUT size for swift
            raise NotImplementedError("large objects are not implemented")
            # see https://docs.openstack.org/swift/latest/api/large_objects.html#static-large-objects
            # and https://docs.openstack.org/api-ref/object-store

        url = ref.http_url
        headers = self.headers_for_url(url)
        headers["Content-Length"] = str(size)
        if self.verify_uploads:
            # in swift, ETag is alwas the MD5sum and will be used by the server to verify the upload
            headers["ETag"] = md5(data).hexdigest()

        session = await self.set_session()
        async with session.put(url, data=data, headers=headers) as res:
            res.raise_for_status()

    async def _rm_file(self, path, **kwargs):
        ref = SWIFTRef(path)
        if not ref.object:
            raise NotImplementedError("currently rm is only implemented for objects")
        headers = self.headers_for_url(ref.http_url)
        session = await self.set_session()
        async with session.delete(ref.http_url, headers=headers) as res:
            res.raise_for_status()

    def _open(
        self,
        path,
        mode="rb",
        block_size=None,
        autocommit=True,
        cache_options=None,
        **kwargs,
    ):
        """Return raw bytes-mode file-like from the file-system"""
        return SWIFTFile(
            self,
            path,
            mode,
            block_size,
            autocommit,
            cache_options=cache_options,
            **kwargs,
        )

    def ukey(self, path):
        return tokenize(path, self.kwargs, self.info(path)["ETag"])

    async def _info(self, path, **kwargs):
        ref = SWIFTRef(path)
        if not ref.object:
            return {
                "name": ref.swift_url,
                "type": "directory",
                "size": None,
            }
        headers = self.headers_for_url(ref.http_url)
        session = await self.set_session()
        async with session.head(ref.http_url, headers=headers) as res:
            if res.status != 200:
                raise FileNotFoundError(f"file '{ref.swift_url}' not found")
            info = {
                "name": ref.swift_url,
                "type": "file",
                "size": int(res.headers["Content-Length"]),
                "Etag": res.headers["Etag"],
            }
        return info
