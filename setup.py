import setuptools
from AccountingKits import __version__ as ak_version

setuptools.setup(
    name='AccountingKits',
    version=ak_version,
    description='Accounting kits packages, especially for banking research',
    author='Zhang Qihang',
    author_email='694499657@qq.com',
    packages=setuptools.find_packages()
)
