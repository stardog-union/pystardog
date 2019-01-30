import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="pystardog",
    version="0.9.0",
    author="Stardog Union",
    author_email="support@stardog.com",
    description="Use Stardog with Python!",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/stardog-union/pystardog",
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: Apache Software License",
        "Operating System :: OS Independent",
    ],
)
