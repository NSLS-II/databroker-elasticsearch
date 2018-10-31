from setuptools import setup, find_packages


setup(
    name='databroker-elasticsearch',
    version='0.0.0',
    packages=find_packages('src'),
    package_dir={'': 'src'},
    description='Databroker ElasticSearch bridge',
    zip_safe=False,
    include_package_data=True,
)
