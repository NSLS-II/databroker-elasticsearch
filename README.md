[![Build Status](https://travis-ci.org/NSLS-II/databroker-elasticsearch.svg?branch=master)](https://travis-ci.org/NSLS-II/databroker-elasticsearch)
[![codecov](https://codecov.io/gh/NSLS-II/databroker-elasticsearch/branch/master/graph/badge.svg)](https://codecov.io/gh/NSLS-II/databroker-elasticsearch)

# databroker-elasticsearch

Integration of elasticsearch with databroker

databroker-elasticsearch provides export bridge of databroker records to
[Elasticsearch](https://www.elastic.co/products/elasticsearch) (ES).
The package provides configurable mapping of databroker documents to
ES records, filtering of permitted entries, facility to export all
prior databroker records and a bluesky callback mechanism to export
new measurements in real time.
