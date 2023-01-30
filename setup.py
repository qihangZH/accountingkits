import setuptools
from accountingkits import __version__ as ak_version

setuptools.setup(
    name='accountingkits',
    version=ak_version,
    description='accountingkits packages, especially for banking research',
    author='zhang qihang',
    author_email='694499657@qq.com',
    packages=setuptools.find_packages()
)
