from setuptools import setup, find_packages

requirement_path = "requirements.txt"

with open(requirement_path, 'r', encoding="utf-8") as f:
    install_requires = f.read().splitlines()

setup(
    name="pseudo_ros",
    version='0.0.1',
    packages=find_packages(),
    install_requires=install_requires,
)