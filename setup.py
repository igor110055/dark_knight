from setuptools import setup

# from pkg_resources import parse_requirements

# with open('requirements.txt') as requirements_text:
#     requirements = parse_requirements(requirements_text.read())

setup(
    name="Storm",
    packages=["storm", "storm.engines", "storm.services", "storm.models"],
    # install_requires=[str(r) for r in requirements]
)
