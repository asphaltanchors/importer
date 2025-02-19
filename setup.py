from setuptools import setup, find_packages

setup(
    name="py-importer",
    version="1.6.0",
    description="CSV import processor for database ingestion",
    packages=find_packages(),
    python_requires=">=3.11",
    install_requires=[
        "pandas>=2.2.0",
        "sqlalchemy>=2.0.0",
        "click>=8.1.0",
        "python-dotenv>=1.0.0",
        "psycopg2-binary>=2.9.0",
        "tld>=0.13",
    ],
    entry_points={
        "console_scripts": [
            "importer=importer.cli:cli",
        ],
    },
)
