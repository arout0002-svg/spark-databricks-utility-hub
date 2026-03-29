from setuptools import find_packages, setup


setup(
    name="spark-databricks-utility-hub",
    version="0.1.0",
    description="PySpark + Databricks project with reusable JAR utilities",
    python_requires=">=3.11",
    package_dir={"": "src"},
    packages=find_packages(where="src"),
    include_package_data=True,
    install_requires=[
        "pyspark==3.5.1",
        "delta-spark==3.2.0",
        "PyYAML==6.0.2",
        "python-json-logger==2.0.7",
    ],
)
