# Changelog

## [1.0.0](https://github.com/googleapis/java-spanner-cassandra/compare/v0.7.0...v1.0.0) (2025-09-25)


### Miscellaneous Chores

* Release version 1.0.0 ([#239](https://github.com/googleapis/java-spanner-cassandra/issues/239)) ([0b24095](https://github.com/googleapis/java-spanner-cassandra/commit/0b24095cc8dbd8d99e95660d91396684966707ed))

## [0.7.0](https://github.com/googleapis/java-spanner-cassandra/compare/v0.6.0...v0.7.0) (2025-09-09)


### Features

* Add &lt;name&gt; to the google-cloud-spanner-cassandra module ([#222](https://github.com/googleapis/java-spanner-cassandra/issues/222)) ([fce916f](https://github.com/googleapis/java-spanner-cassandra/commit/fce916febbbb4a1fb7e2cf881047ba5f3482dcc6))

## [0.6.0](https://github.com/googleapis/java-spanner-cassandra/compare/v0.5.0...v0.6.0) (2025-09-09)


### Features

* Skip deployment for integration tests module ([#219](https://github.com/googleapis/java-spanner-cassandra/issues/219)) ([479ca0a](https://github.com/googleapis/java-spanner-cassandra/commit/479ca0a202b6e39522fb45e05c5224346ca4e5f6))

## [0.5.0](https://github.com/googleapis/java-spanner-cassandra/compare/v0.4.0...v0.5.0) (2025-09-06)


### Features

* Add env var to log server errors ([#198](https://github.com/googleapis/java-spanner-cassandra/issues/198)) ([f06a242](https://github.com/googleapis/java-spanner-cassandra/commit/f06a24242675843e56aad1cc0ec43f3d6318309b))
* Add health check endpoint ([#197](https://github.com/googleapis/java-spanner-cassandra/issues/197)) ([881137a](https://github.com/googleapis/java-spanner-cassandra/commit/881137a06aedeef3321bb75bd8b770374379de67))
* Add YAML config parser ([#208](https://github.com/googleapis/java-spanner-cassandra/issues/208)) ([2f5c062](https://github.com/googleapis/java-spanner-cassandra/commit/2f5c062e3f59477ac700355e52c8027c9020b42a))
* Handle OPTIONS message locally ([#209](https://github.com/googleapis/java-spanner-cassandra/issues/209)) ([708d7ab](https://github.com/googleapis/java-spanner-cassandra/commit/708d7ab6a348e5c19c60f6175bff18b550912f9f))
* Support YAML configuration for Launcher ([#211](https://github.com/googleapis/java-spanner-cassandra/issues/211)) ([15d2797](https://github.com/googleapis/java-spanner-cassandra/commit/15d2797570e7de3725cd3e30eea899c5401d30c6))


### Bug Fixes

* Add missing setting for direct path enablement ([#145](https://github.com/googleapis/java-spanner-cassandra/issues/145)) ([5099d98](https://github.com/googleapis/java-spanner-cassandra/commit/5099d9893dbe160de15e09d35cc95be2839ecc71))
* Add null check for healthcheck server ([#202](https://github.com/googleapis/java-spanner-cassandra/issues/202)) ([c4390f5](https://github.com/googleapis/java-spanner-cassandra/commit/c4390f5a15034bfe35bc14d7ae7a4a363c8fd231))
* Attach appropriate stream_id for error response ([#149](https://github.com/googleapis/java-spanner-cassandra/issues/149)) ([54ba573](https://github.com/googleapis/java-spanner-cassandra/commit/54ba57305d659d52f007e0ce602ca9a5a3031ebc))
* Correct method name ([#127](https://github.com/googleapis/java-spanner-cassandra/issues/127)) ([0a5fa64](https://github.com/googleapis/java-spanner-cassandra/commit/0a5fa64f9b172db49c126bce33246dee38df848c))
* Create custom serverFrameCodec wiith DseProtocolV2ServerCodecs t… ([#205](https://github.com/googleapis/java-spanner-cassandra/issues/205)) ([61292d6](https://github.com/googleapis/java-spanner-cassandra/commit/61292d65a2941c3ac69bc18c6d926cf20869067f))
* Fix lint ([#184](https://github.com/googleapis/java-spanner-cassandra/issues/184)) ([177b45c](https://github.com/googleapis/java-spanner-cassandra/commit/177b45cc9c6239c4592925e6c334e72502c304e0))


### Performance Improvements

* Add support for virtual threads ([#169](https://github.com/googleapis/java-spanner-cassandra/issues/169)) ([393ae0a](https://github.com/googleapis/java-spanner-cassandra/commit/393ae0a1a80c4790cddc1be6d265f4fd95960da6))
* Directly write ByteString received from gRPC response to socket to avoid copies ([#174](https://github.com/googleapis/java-spanner-cassandra/issues/174)) ([8d5a2b7](https://github.com/googleapis/java-spanner-cassandra/commit/8d5a2b7f14b8f6c27dbfc53fecab25644f7857be))
* Only decode bytes to frame for query, execute and batch messages ([#163](https://github.com/googleapis/java-spanner-cassandra/issues/163)) ([b0f279d](https://github.com/googleapis/java-spanner-cassandra/commit/b0f279d9b89c86e29210ff192b3ef85be9f0feba))
* Small optimizations to reduce CPU overhead on hot paths ([#190](https://github.com/googleapis/java-spanner-cassandra/issues/190)) ([f0d9777](https://github.com/googleapis/java-spanner-cassandra/commit/f0d9777426241fbc35e2051432e41bdeb8ab786b))


### Documentation

* Add cqlsh instructions ([#187](https://github.com/googleapis/java-spanner-cassandra/issues/187)) ([bb8dec9](https://github.com/googleapis/java-spanner-cassandra/commit/bb8dec9ab3c95f7e793b82a82f94521732ef4995))
* Add YCSB doc ([#173](https://github.com/googleapis/java-spanner-cassandra/issues/173)) ([e2703b4](https://github.com/googleapis/java-spanner-cassandra/commit/e2703b47f7ec22c7e1c6fe5003496f633210bf5f))
* View and manage client-side metrics ([#188](https://github.com/googleapis/java-spanner-cassandra/issues/188)) ([3d2b44b](https://github.com/googleapis/java-spanner-cassandra/commit/3d2b44b666f530fd9fdfbd1e3cc4aacad3811d40))

## [0.4.0](https://github.com/googleapis/java-spanner-cassandra/compare/v0.3.0...v0.4.0) (2025-06-29)


### Features

* Add support for setting spanner host ([#122](https://github.com/googleapis/java-spanner-cassandra/issues/122)) ([9ed5c49](https://github.com/googleapis/java-spanner-cassandra/commit/9ed5c49d7d1c4706aabae1b3ca52da201264c1bf))
* Add support for setting the max commit delay ([#115](https://github.com/googleapis/java-spanner-cassandra/issues/115)) ([1945467](https://github.com/googleapis/java-spanner-cassandra/commit/19454675d3dff0445a0a3be24feed20559337a34))
* Enable Leader-Aware Routing by default for all write operations ([#104](https://github.com/googleapis/java-spanner-cassandra/issues/104)) ([d8c4e47](https://github.com/googleapis/java-spanner-cassandra/commit/d8c4e47cac549e83e23e1b42326d654225f1cfb9))


### Bug Fixes

* Add missing dependencies in samples packcage ([#73](https://github.com/googleapis/java-spanner-cassandra/issues/73)) ([f9208b2](https://github.com/googleapis/java-spanner-cassandra/commit/f9208b23ead09cb188d8cf53634b417653725e06))


### Documentation

* Add unsupported features from apache/cassandra-java-driver ([#42](https://github.com/googleapis/java-spanner-cassandra/issues/42)) ([3df0067](https://github.com/googleapis/java-spanner-cassandra/commit/3df00673d3303453d56da99c24f5eb36eae59155))
* Correct developers ([#75](https://github.com/googleapis/java-spanner-cassandra/issues/75)) ([83d23f3](https://github.com/googleapis/java-spanner-cassandra/commit/83d23f3f78a9fc8b74f5061007afc15d1542ede1))

## [0.3.0](https://github.com/googleapis/java-spanner-cassandra/compare/v0.2.0...v0.3.0) (2025-05-15)


### Features

* Add support for direct path ([#30](https://github.com/googleapis/java-spanner-cassandra/issues/30)) ([9dbb0cf](https://github.com/googleapis/java-spanner-cassandra/commit/9dbb0cfa005f7126488a837608cda1250eefff23))


### Documentation

* Add comment to automatically update dependencies in spanner docs ([#28](https://github.com/googleapis/java-spanner-cassandra/issues/28)) ([260cb81](https://github.com/googleapis/java-spanner-cassandra/commit/260cb81bbc297cedb4681b7f8838515ed6c01c61))

## [0.2.0](https://github.com/googleapis/java-spanner-cassandra/compare/v0.1.0...v0.2.0) (2025-05-08)


### Features

* Add RLS header ([#23](https://github.com/googleapis/java-spanner-cassandra/issues/23)) ([72a5fe0](https://github.com/googleapis/java-spanner-cassandra/commit/72a5fe0c6044210522ac14b2a3be3cc59498b5d1))
* Implement response stitching ([#20](https://github.com/googleapis/java-spanner-cassandra/issues/20)) ([83afe77](https://github.com/googleapis/java-spanner-cassandra/commit/83afe77624dd589c8dec5eaa441dc16dbc7bd940))


### Dependencies

* Update dependency com.google.cloud:sdk-platform-java-config to v3.47.0 ([#18](https://github.com/googleapis/java-spanner-cassandra/issues/18)) ([8d4678e](https://github.com/googleapis/java-spanner-cassandra/commit/8d4678e12100e3b7280865dc0bcaf047ef82e623))
* Update dependency org.mockito:mockito-inline to v5 ([#9](https://github.com/googleapis/java-spanner-cassandra/issues/9)) ([8b12078](https://github.com/googleapis/java-spanner-cassandra/commit/8b1207884b08cbb8b2084a47e147363860e47d6e))
* Update dependency org.slf4j:slf4j-api to v2 ([#11](https://github.com/googleapis/java-spanner-cassandra/issues/11)) ([4225c6b](https://github.com/googleapis/java-spanner-cassandra/commit/4225c6b022c99ca59f9dcfc0082f20b823ec1559))
* Update dependency org.slf4j:slf4j-simple to v2 ([#12](https://github.com/googleapis/java-spanner-cassandra/issues/12)) ([5dd1242](https://github.com/googleapis/java-spanner-cassandra/commit/5dd124288ba705d98af5dc586c080c2f66385312))


### Documentation

* Automatically update versions in readme on release ([#15](https://github.com/googleapis/java-spanner-cassandra/issues/15)) ([312af18](https://github.com/googleapis/java-spanner-cassandra/commit/312af18b49b6004f40d4eceaa3419b2e0042cd10))
* Format README and table comment ([#21](https://github.com/googleapis/java-spanner-cassandra/issues/21)) ([1d8b102](https://github.com/googleapis/java-spanner-cassandra/commit/1d8b1028598fc3cef73bd8c4a2bb55b4bb577204))

## 0.1.0 (2025-04-08)


### Features

* Initial commit ([884a337](https://github.com/googleapis/java-spanner-cassandra/commit/884a337eee307ed1d154cce35fb2067cbd95c8b7))
