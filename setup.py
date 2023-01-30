import setuptools
from accounting_kits import __version__ as ak_version

setuptools.setup(
    name='accounting_kits',
    version=ak_version,
    description='accounting_kits packages, especially for banking research',
    author='zhang qihang',
    author_email='694499657@qq.com',
    packages=setuptools.find_packages()
)
