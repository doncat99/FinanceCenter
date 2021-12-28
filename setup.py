from setuptools import find_packages, setup

# Read requirements.txt, ignore comments
try:
    REQUIRES = list()
    f = open("requirements.txt", "rb")
    for line in f.read().decode("utf-8").split("\n"):
        line = line.strip()
        if "#" in line:
            line = line[: line.find("#")].strip()
        if line:
            REQUIRES.append(line)
except FileNotFoundError:
    print("'requirements.txt' not found!")
    REQUIRES = list()

setup(
    name="findy",
    version="0.0.2",
    include_package_data=True,
    author="Don Wong, Bin Wong",
    author_email="doncat99@gmail.com",
    url="https://github.com/doncat99/FinanceCenter",
    license="MIT",
    packages=find_packages(),
    description="FinDy, fetching open financing data library",
    long_description="FinDy, fetching open financing data library",
    classifiers=[
        # Trove classifiers
        # Full list: https://pypi.python.org/pypi?%3Aaction=list_classifiers
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3.8",
    ],
    keywords="Finance Data",
    platform=["mac", "linux"],
    python_requires="==3.8.12",
)
