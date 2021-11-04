from pkg_resources import parse_requirements
from setuptools import setup

with open('requirements.txt') as requirements_text:
    requirements = parse_requirements(requirements_text.read())

setup(
    name='Storm',
    packages=['storm'],
    install_requires=[str(r) for r in requirements]
)
