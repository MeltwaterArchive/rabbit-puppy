# rabbit-puppy Release Notes
## next
### Changes
- No changes

## 0.4.0 - 2018-03-04
### Changes
- Uploads java artifacts to meltwater.jfrog.io
- Uses amqp-client 3.6.6 and kotlin 1.2.31
- Updated all jar dependencies and all maven plugin versions
- Handles duplicate keys in RabbitMQ management API responses
- Fixed failng tests by changing from NO_CONTENT to CREATED http codes - the RabbitMQ rest API has changed

## 0.3.0 - 2016-03-09
### Changes
- Upgrades Kotlin to 1.0.0

## 0.2.3 - 2016-02-09
### Fixes
- Fix crash when specifying empty binding list for an exchange

## v0.2.2 - 2016-01-13
### Fixes
- Increased stability of docker integration during tests

## v0.2.1 - 2016-01-13
### Fixes
- Fixes #14 x-messages-ttl is parsed as Double

## v0.2.0 - 2015-12-07
### Changes
- Added `apply` and `verify` commands
- Revert changes made to Java API after switch to Kotlin
- Must specify empty hash if no properties specified e.g. `arguments: {}`

### Fixes
- More descriptive and compact logging
- Fixed several null-pointer errors

## v0.1.4 - 2015-12-03
### Fixes
- Issue when choosing user to configure resource with

## v0.1.3 - 2015-12-03
### Fixes
- Main class error when running jar
- Kotlin build error

## 0.1.2 - 2015-12-01
### Changes
- Converted to Kotlin

## 0.1.1 - 2015-11-24
### Changes
- Removed unused dependency
- Tag docker images with correct version

## 0.1.0 - 2015-11-24
- First release!
