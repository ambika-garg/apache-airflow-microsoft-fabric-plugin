from setuptools import setup, find_packages
import pathlib

# The directory containing this file
HERE = pathlib.Path(__file__).parent

# The text of the README file
README = (HERE / "README.md").read_text()

setup(
    name="apache_airflow_microsoft_fabric_plugin",
    version="1.0.0a0",  # Alpha release
    author="Ambika Garg",
    author_email="ambikagarg1101@gmail.com",
    description="A plugin for Apache Airflow to interact with Microsoft Fabric items",
    long_description=README,
    long_description_content_type="text/markdown",  # Assuming README.md is in Markdown
    url="https://github.com/ambika-garg/apache-airflow-microsoft-fabric-plugin.git",
    packages=find_packages(),  # Automatically find packages in the directory
    classifiers=[
        "Programming Language :: Python :: 3",
        "Development Status :: 3 - Alpha",
        "Environment :: Plugins",
        "Intended Audience :: Developers",
        "Topic :: Software Development :: Libraries :: Python Modules",
    ],
    python_requires='>=3.8',
)
