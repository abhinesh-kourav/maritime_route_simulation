from setuptools import find_packages, setup
from typing import List

REQUIREMENTS_FILENAME = 'requirements.txt'


def get_requirements_list()->List[str]:
    """
    This function is going to return list of requirements present in requirements.txt file

    returns a list of all library names needed to be installed to run the app.
    """
    with open(REQUIREMENTS_FILENAME, 'r') as requirements_file:
        return requirements_file.readlines().remove('-e .')


from setuptools import setup, find_packages

setup(
    name="maritime_route_simulation",
    version="0.1.0",
    packages=find_packages(),
    install_requires=get_requirements_list(),
    author="Abhinesh Kourav",
    description="A simulation and data engineering tool for maritime AIS data",
)