import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="pslx",
    version="0.0.1",
    author="Francis Chen",
    author_email="kfrancischen@gmail.com",
    description="Python Standard Library eXtension",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/kfrancischen/pslx",
    packages=setuptools.find_packages(),
    include_package_data=True,
    zip_safe=False,
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: GNU GENERAL PUBLIC LICENSE",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.7',
)
