# Release notes

## Version 0.0.2 – 2019-06-07

### Added

- `BrokerSearch` - to retrieve databroker headers with ES query.
- Support epochseconds as `date` field value or limit in ES queries.
- `makewheel` script to produce binary-reproducible package.

### Changed

- Make Lucene query argument `q` optional in `ElasticIndex.qsearch` and
  add `query` argument for full ES DSL queries.
- Record local timezone in ES `date` field.j


## Version 0.0.1 -- 2019-02-22

### Added

- `ElasticDocument` - transform run-engine document to Elasticsearch entry.
- `ElasticIndex` - convenience utility class for accessing Elasticsearch.
- `ElasticCallback` - lightweight specialization of bluesky `CallbackBase`
  for exporting bluesky "start" documents to ES.
- Facility to export past databroker records to ES.
- Test coverage for all sources and an automated CI tests on travis.
- Recipe for platform-independent Anaconda package.
