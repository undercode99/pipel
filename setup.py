import setuptools
with open("README.md", "r") as fh:
    long_description = fh.read()


setuptools.setup(
    name="pipel",
    version="1.0.0",
    author="zeroblack-c",
    author_email="zrcode101@gmail.com",
    description="Simple Data Pipeline Management",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/zeroblack-c/pipel",
    packages=setuptools.find_packages(),
    install_requires=[
        'docopt==0.6.2'
    ],
    entry_points={
        'console_scripts': [
            'pipel = pipel.cli:main',
        ],
    },
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
)