import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="pslx",
    version="0.0.1",
    author="Francis Chen",
    author_email="kfrancischen@gmail.com",
    description="Python Standard Library eXtension",
    long_description="This package is aimed to provide libraries for customized functions.",
    long_description_content_type="text/markdown",
    url="https://github.com/kfrancischen/pslx",
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: GNU GENERAL PUBLIC LICENSE",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.7',
)
