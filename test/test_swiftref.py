import pytest
from swiftspec.core import SWIFTRef, swift_res_to_info

object_test_urls = [
    "swift://server/a/c/f/test.txt",
    "https://server/v1/a/c/f/test.txt",
]


@pytest.mark.parametrize("url", object_test_urls)
def test_parse_object(url):
    ref = SWIFTRef(url)
    assert ref.host == "server"
    assert ref.account == "a"
    assert ref.container == "c"
    assert ref.object == "f/test.txt"
    assert ref.http_url == "https://server/v1/a/c/f/test.txt"


container_test_urls = [
    "swift://server/a/c",
    "https://server/v1/a/c",
    "swift://server/a/c/",
    "https://server/v1/a/c/",
]


@pytest.mark.parametrize("url", container_test_urls)
def test_parse_container(url):
    ref = SWIFTRef(url)
    assert ref.host == "server"
    assert ref.account == "a"
    assert ref.container == "c"
    assert ref.object is None
    assert ref.http_url == "https://server/v1/a/c"


info_examples = [
    (
        "swift://s/a/c/",
        {"subdir": "f/"},
        {"name": "swift://s/a/c/f", "size": None, "type": "directory"},
    ),
    (
        "swift://s/a/c/",
        {"subdir": "f/folder2/"},
        {"name": "swift://s/a/c/f/folder2", "size": None, "type": "directory"},
    ),
    (
        "swift://s/a/c/",
        {
            "bytes": 1179,
            "last_modified": "2021-11-10T13:42:02.919330",
            "hash": "1172d26440cc92b0a77b8324829dabcc",
            "name": "f/test.txt",
            "content_type": "application/octet-stream",
        },
        {
            "name": "swift://s/a/c/f/test.txt",
            "size": 1179,
            "type": "file",
            "hash": "1172d26440cc92b0a77b8324829dabcc",
            "last_modified": "2021-11-10T13:42:02.919330",
            "content_type": "application/octet-stream",
        },
    ),
]


@pytest.mark.parametrize("prefix,res,info", info_examples)
def test_swift_res_to_info(prefix, res, info):
    assert swift_res_to_info(prefix, res) == info
