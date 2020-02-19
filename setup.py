from io import open
from os import path

from setuptools import find_packages, setup

here = path.abspath(path.dirname(__file__))

with open(path.join(here, "README.md"), encoding="utf-8") as f:
    long_description = f.read()

setup(
    name="py-mpworker",
    version="1.1.0",
    description="Python multiprocess periodic worker",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/Ujang360/py-mpworker",
    author="Aditya Kresna",
    author_email="aditya.kresna@outlook.co.id",
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "Topic :: Software Development :: Libraries :: Python Modules",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3.6",
    ],
    keywords="multiprocess",
    packages=find_packages(exclude=["contrib", "docs", "tests"]),  # Required
    python_requires=">=3.6.*, <4",
    install_requires=[],  # Optional
    project_urls={
        "Bug Reports": "https://github.com/Ujang360/py-mpworker/issues",
        "Source": "https://github.com/Ujang360/py-mpworker",
    },
)
