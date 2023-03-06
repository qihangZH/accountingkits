import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name='accountingkits',
    description='The kit-package which made for accounting science research',
    author='zhang qihang',
    author_email='694499657@qq.com',
    long_description=long_description,
    long_description_content_type="text/markdown",
    packages=setuptools.find_packages(),
    python_requires=">=3.7",
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    install_requires=['numpy',
                      'pandas',
                      'pathos',
                      'requests',
                      'python-Levenshtein',
                      'thefuzz',
                      'rapidfuzz',
                      'sas7bdat',
                      'nltk',
                      'beautifulsoup4',
                      'fake-useragent',
                      'Cython']
)
