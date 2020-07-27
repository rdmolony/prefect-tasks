from setuptools import setup, find_packages

with open("README.md", "r") as fh:
    long_description = fh.read()

# base requirements
install_requires = open("requirements.txt").read().strip().split("\n")

setup(
    name="prefect_toolkit",
    packages=find_packages("src"),
    package_dir={"": "src"},
    version="0.0.1",
    author="Codema",
    author_email="rowan.molony@codema.ie",
    description="A collection of prefect tasks...",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/rdmolony/prefect-toolkit",
    classifiers=[
        "Programming Language :: Python :: 3",
        "Operating System :: OS Independent",
    ],
    install_requires=install_requires,
    python_requires=">=3.6",
)

