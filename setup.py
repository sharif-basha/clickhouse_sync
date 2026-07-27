from setuptools import find_packages, setup

setup(
    name="clickhouse_sync",
    version="1.7.0",
    description="Sync ERPNext DocTypes to ClickHouse with alerts and analytics.",
    packages=find_packages(),
    include_package_data=True,
    zip_safe=False,
    python_requires=">=3.10",
    install_requires=["clickhouse-connect>=0.7.0", "requests>=2.28.0"],
)
