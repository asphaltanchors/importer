from setuptools import find_packages, setup

setup(
    name="dqi",
    packages=find_packages(exclude=["dqi_tests"]),
    install_requires=[
        "dagster",
        "dagster-cloud"
    ],
    extras_require={"dev": ["dagster-webserver", "pytest"]},
)
