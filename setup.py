from setuptools import setup, find_packages


setup(
    name='databroker-elasticsearch',
    version='0.3.6',
    packages=find_packages(),
    description='Databroker ElasticSearch bridge',
    zip_safe=False,
    include_package_data=True,
)
