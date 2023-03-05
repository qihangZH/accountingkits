import setuptools
from accountingkits import __version__ as ak_version

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name='accountingkits',
    version=ak_version,
    description='The package which made for science research in accounting, Biz school',
    author='zhang qihang',
    author_email='694499657@qq.com',
    long_description=long_description,
    long_description_content_type="text/markdown",
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
)
