from setuptools import find_packages, setup
from leclub3pkg import __version__

setup(
    name="leclub3pkg",
    packages=find_packages(exclude=["tests", "tests.*"]),
    setup_requires=["wheel"],
    version=__version__,
    description="",
    author=""
)
