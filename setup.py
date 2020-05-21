import setuptools
from setuptools.command.install import install
import subprocess


class CustomInstallCommand(install):

    def run(self):
        subprocess.check_call(['chmod', '+x', 'compile_protos.sh'])
        subprocess.check_call(['./compile_protos.sh'])
        subprocess.check_call(['chmod', '+x', 'run_unittests.sh'])
        subprocess.check_call(['./run_unittests.sh'])
        install.run(self)


with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="pslx",
    version="0.8",
    scripts=['compile_protos.sh', 'run_unittests.sh'],
    cmdclass={
        'install': CustomInstallCommand,
    },
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
