import setuptools

import versioneer

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="swiftspec",
    version=versioneer.get_version(),
    cmdclass=versioneer.get_cmdclass(),
    author="Tobias Kölling",
    author_email="tobias.koelling@mpimet.mpg.de",
    description="fsspec implementation for OpenStack SWIFT",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/d70-t/swiftspec",
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires=">=3.7",
    install_requires=[
        "fsspec>=0.9.0",
        "aiohttp",
    ],
    entry_points={
        "fsspec.specs": [
            "swift=swiftspec.SWIFTFileSystem",
        ],
    },
)
