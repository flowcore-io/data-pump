# Changelog

## [0.15.0](https://github.com/flowcore-io/data-pump/compare/v0.14.0...v0.15.0) (2025-06-03)


### Features

* **data-pump:** :sparkles: add acknowledgment timeout scaling and timeout management ([17ea18d](https://github.com/flowcore-io/data-pump/commit/17ea18d6cef86ad73a77158a1008ce353e3471bb))


### Bug Fixes

* **data-pump:** :art: reverted concurrency changes ([0f0b29c](https://github.com/flowcore-io/data-pump/commit/0f0b29ce5a807cd5bf0e6a44798310ee409adab1))
* **data-pump:** :art: update timeoutId type to use ReturnType of setTimeout ([132aba2](https://github.com/flowcore-io/data-pump/commit/132aba2153e70b8532fab3cfc53816e2d9450c21))
* **deno:** :art: update deno.lock version and format deno.json ([9bf22c7](https://github.com/flowcore-io/data-pump/commit/9bf22c767f3e2c3c413386d38e4f2d60cdd586f8))

## [0.14.0](https://github.com/flowcore-io/data-pump/compare/v0.13.0...v0.14.0) (2025-06-03)


### Features

* **data-pump:** :sparkles: add acknowledgment timeout scaling and timeout management ([64fdc7e](https://github.com/flowcore-io/data-pump/commit/64fdc7e099820922d3ae5f8eee42308d427069dc))


### Bug Fixes

* **data-pump:** :art: update timeoutId type to use ReturnType of setTimeout ([96ce40c](https://github.com/flowcore-io/data-pump/commit/96ce40c0d2ecdf564a5d63b12a6f948b56f4ab41))
* **deno:** :art: update deno.lock version and format deno.json ([80f634e](https://github.com/flowcore-io/data-pump/commit/80f634ef88626ab9476890076e96212611ebb1c0))

## [0.13.0](https://github.com/flowcore-io/data-pump/compare/v0.12.4...v0.13.0) (2025-05-01)


### Features

* **dependencies:** :sparkles: update @flowcore/sdk to version 1.38.0 added include sensitive data functionality ([0bfc953](https://github.com/flowcore-io/data-pump/commit/0bfc953e82ca1e704c4d2d89a5783bebb69cbdbb))

## [0.12.4](https://github.com/flowcore-io/data-pump/compare/v0.12.3...v0.12.4) (2025-05-01)


### Bug Fixes

* update sdk ([339d28d](https://github.com/flowcore-io/data-pump/commit/339d28d5d0700971221c44990e86d51004fd4c7d))

## [0.12.3](https://github.com/flowcore-io/data-pump/compare/v0.12.2...v0.12.3) (2025-05-01)


### Bug Fixes

* fix for fetch events ([b3444db](https://github.com/flowcore-io/data-pump/commit/b3444db7d95bf61114c2c593825322e5e7925b07))

## [0.12.2](https://github.com/flowcore-io/data-pump/compare/v0.12.1...v0.12.2) (2025-04-30)


### Bug Fixes

* **dependencies:** :arrow_up: update @flowcore/sdk to version 1.36.1 ([9386a69](https://github.com/flowcore-io/data-pump/commit/9386a691e2b602fbe9691667b80f6f88f176e2de))
* **dependencies:** :sparkles: add @flowcore/sdk version 1.36.1 to deno.lock ([feb3c86](https://github.com/flowcore-io/data-pump/commit/feb3c86cdf30969a54635ff2914a80f46a45207c))

## [0.12.1](https://github.com/flowcore-io/data-pump/compare/v0.12.0...v0.12.1) (2025-04-16)


### Bug Fixes

* fix pagination of events ([2ef1af8](https://github.com/flowcore-io/data-pump/commit/2ef1af835c1083560fdcb4521153f448ae56eed7))
* fix pagination of events ([d0b3301](https://github.com/flowcore-io/data-pump/commit/d0b3301aef82c2ae7b357d8287fb4ed9cc4e6ef2))

## [0.12.0](https://github.com/flowcore-io/data-pump/compare/v0.11.0...v0.12.0) (2025-04-14)


### Features

* remove old datapump code ([d940e25](https://github.com/flowcore-io/data-pump/commit/d940e25bf9e5c67a6609d33f741c23b3dad3ca23))


### Bug Fixes

* add 1 sec delay when rechecking for events in livemode ([024a9e0](https://github.com/flowcore-io/data-pump/commit/024a9e09dbd9998e36257f29c154d50ed82ff7a4))
* add no op logger ([fd83978](https://github.com/flowcore-io/data-pump/commit/fd83978275b1102b80c853dd53c00764b747a9c7))

## [0.11.0](https://github.com/flowcore-io/data-pump/compare/v0.10.0...v0.11.0) (2025-04-14)


### Features

* **data-pump:** :sparkles: enhance FlowcoreDataSource with detailed JSDoc comments ([1e3308d](https://github.com/flowcore-io/data-pump/commit/1e3308d6c1aca7da47c96650f0b1e9ef9c8b7c8a))

## [0.10.0](https://github.com/flowcore-io/data-pump/compare/v0.9.0...v0.10.0) (2025-04-14)


### Features

* **data-pump:** :sparkles: export data source module ([6208e42](https://github.com/flowcore-io/data-pump/commit/6208e42bbf59e67d70235833a08d6e116b29dc60))

## [0.9.0](https://github.com/flowcore-io/data-pump/compare/v0.8.3...v0.9.0) (2025-04-14)


### Features

* **data-pump:** :sparkles: allow data source override in create method ([4af96f1](https://github.com/flowcore-io/data-pump/commit/4af96f1daa75842abbe9eb9f241b8bc22cc76d53))


### Bug Fixes

* :rotating_light: fixed linting errors ([a0304a3](https://github.com/flowcore-io/data-pump/commit/a0304a3abf0f58cc53ecb584869bb4a94c6b5d66))

## [0.8.3](https://github.com/flowcore-io/data-pump/compare/v0.8.2...v0.8.3) (2025-04-09)


### Bug Fixes

* change runners ([cea7bdf](https://github.com/flowcore-io/data-pump/commit/cea7bdfac67ad5bda9643ad6b369b9a7630de4ae))
* fix for stopat ([985d5fb](https://github.com/flowcore-io/data-pump/commit/985d5fb6f5afed3291216131583db0ac11a4384e))

## [0.8.2](https://github.com/flowcore-io/data-pump/compare/v0.8.1...v0.8.2) (2025-04-02)


### Bug Fixes

* **dependencies:** :arrow_up: update @flowcore/sdk to version 1.24.4 ([986ff61](https://github.com/flowcore-io/data-pump/commit/986ff61f8cb4eb31e198b9873d1f085284562faa))

## [0.8.1](https://github.com/flowcore-io/data-pump/compare/v0.8.0...v0.8.1) (2025-04-02)


### Bug Fixes

* **flowcore-client:** :poop: add directMode parameter to execute method ([0cf88ba](https://github.com/flowcore-io/data-pump/commit/0cf88ba4e82092a992d0b68b4eff5207322961ea))

## [0.8.0](https://github.com/flowcore-io/data-pump/compare/v0.7.2...v0.8.0) (2025-04-02)


### Features

* **data-source:** :poop: add directMode and noTranslation options to FlowcoreNotifier ([fabc73e](https://github.com/flowcore-io/data-pump/commit/fabc73ebeff27a34381504c7f100ecddfaf36a34))

## [0.7.2](https://github.com/flowcore-io/data-pump/compare/v0.7.1...v0.7.2) (2025-04-02)


### Bug Fixes

* **data-source:** :poop: add debug logs for options and translations ([ec93b67](https://github.com/flowcore-io/data-pump/commit/ec93b675eaab743d5dd1aba7842c1aa2fa6e6474))

## [0.7.1](https://github.com/flowcore-io/data-pump/compare/v0.7.0...v0.7.1) (2025-04-02)


### Bug Fixes

* **deno:** :poop: update Flowcore SDK to version 1.24.3 ([b830721](https://github.com/flowcore-io/data-pump/commit/b830721d1b6c61dae7682948073224b854d784f4))

## [0.7.0](https://github.com/flowcore-io/data-pump/compare/v0.6.0...v0.7.0) (2025-04-02)


### Features

* **data-source:** :sparkles: add directMode option to FlowcoreDataPump and FlowcoreDataSource ([4d199f3](https://github.com/flowcore-io/data-pump/commit/4d199f392e4a9e755661b5182982cc62d8c261be))


### Bug Fixes

* **deno.lock:** :sparkles: add rxjs dependency to Flowcore SDK ([9883816](https://github.com/flowcore-io/data-pump/commit/98838161c44b33b276d485a6e3595c70220865d9))

## [0.6.0](https://github.com/flowcore-io/data-pump/compare/v0.5.0...v0.6.0) (2025-04-02)


### Features

* **data-source:** :sparkles: add noTranslation option to FlowcoreDataPump and FlowcoreDataSource ([692b79e](https://github.com/flowcore-io/data-pump/commit/692b79e2f1b07b1300a7ab016ec39efb23dc578b))

## [0.5.0](https://github.com/flowcore-io/data-pump/compare/v0.4.2...v0.5.0) (2025-04-02)


### Features

* **data-pump:** :sparkles: add baseUrlOverride option to FlowcoreDataPump and FlowcoreDataSource ([ab898b4](https://github.com/flowcore-io/data-pump/commit/ab898b4f0550f5e56fe4d2d794c5d70d8673b7cc))
* **data-source:** :sparkles: add urlOverride option to FlowcoreDataSource ([b72f8ad](https://github.com/flowcore-io/data-pump/commit/b72f8ade4c521657aaebe4861a2ff10b16fed7d5))

## [0.4.2](https://github.com/flowcore-io/data-pump/compare/v0.4.1...v0.4.2) (2025-03-28)


### Bug Fixes

* fix for fetching event type ids ([95770d7](https://github.com/flowcore-io/data-pump/commit/95770d794dbe177778b3c2fb6318601e6106d73e))

## [0.4.1](https://github.com/flowcore-io/data-pump/compare/v0.4.0...v0.4.1) (2025-03-27)


### Bug Fixes

* add fail for failing events ([96f67e4](https://github.com/flowcore-io/data-pump/commit/96f67e49650c8f81dbd65831ab77adca490bb353))
* update lock file ([1c12ce4](https://github.com/flowcore-io/data-pump/commit/1c12ce49cafee0f6ecf3d144b262b4fbda6c9d29))
* update SDK ([9dfe77a](https://github.com/flowcore-io/data-pump/commit/9dfe77a608f43791b953b233a9f99c9b8e615cb1))

## [0.4.0](https://github.com/flowcore-io/data-pump/compare/v0.3.5...v0.4.0) (2025-03-26)


### Features

* poller notifier and failedHandler ([e93cf5b](https://github.com/flowcore-io/data-pump/commit/e93cf5ba37f5236d21a7fc9722795fa8f563f97b))

## [0.3.5](https://github.com/flowcore-io/data-pump/compare/v0.3.4...v0.3.5) (2025-03-26)


### Bug Fixes

* was starting at wrong bucket ([542fa8e](https://github.com/flowcore-io/data-pump/commit/542fa8e73ddcb0ef4d4c85ba95d88a693149cc5e))

## [0.3.4](https://github.com/flowcore-io/data-pump/compare/v0.3.3...v0.3.4) (2025-03-24)


### Bug Fixes

* timebuckets fetch fix ([b3df2fd](https://github.com/flowcore-io/data-pump/commit/b3df2fd529415e978f5143c9b7ffd5eea68130f2))

## [0.3.3](https://github.com/flowcore-io/data-pump/compare/v0.3.2...v0.3.3) (2025-03-24)


### Bug Fixes

* fix stopat ([54e3aa0](https://github.com/flowcore-io/data-pump/commit/54e3aa0c2fb73b278d3de3299872c7b726de12dc))
* update lock file ([6682c5b](https://github.com/flowcore-io/data-pump/commit/6682c5bb9157c4e60a63a5c8b0b6d94f65926d57))
* upgrade flowcore sdk ([29a65f8](https://github.com/flowcore-io/data-pump/commit/29a65f8ea7a70b91b4c20df00b67b60d73a5557d))

## [0.3.2](https://github.com/flowcore-io/data-pump/compare/v0.3.1...v0.3.2) (2025-03-24)


### Bug Fixes

* fix for waitForBufferEmpty ([e6c6463](https://github.com/flowcore-io/data-pump/commit/e6c64639ed4a6b5e9b47921185c26d6eea03792b))
* update readme ([c9416a8](https://github.com/flowcore-io/data-pump/commit/c9416a8882617c4b4c0ddda714d82906321b85ca))

## [0.3.1](https://github.com/flowcore-io/data-pump/compare/v0.3.0...v0.3.1) (2025-03-20)


### Bug Fixes

* fix stopat ([6586132](https://github.com/flowcore-io/data-pump/commit/6586132e920cdd68b8450c4bb631fef394a284f6))
* fix stopat ([fabb15d](https://github.com/flowcore-io/data-pump/commit/fabb15d616c34340ef4928fa7a1828bc57b8cdd0))

## [0.3.0](https://github.com/flowcore-io/data-pump/compare/v0.2.1...v0.3.0) (2025-03-19)


### Features

* new data pump ([ead3bca](https://github.com/flowcore-io/data-pump/commit/ead3bca138a38df06f9eb306da6c27de5a5d2b39))


### Bug Fixes

* changes ([ca9b28b](https://github.com/flowcore-io/data-pump/commit/ca9b28b7cfd0e9c110fdc42c0f07771215516702))
* diverse fixes ([4f8928d](https://github.com/flowcore-io/data-pump/commit/4f8928dbabe8460290ec44abe49d045952207188))
* run npm validate last ([f5ca2dd](https://github.com/flowcore-io/data-pump/commit/f5ca2dd8c9571b334f30371083b2df499d1e79bf))
* set timer type to any ([846bb71](https://github.com/flowcore-io/data-pump/commit/846bb7188df2be9af7343d66e10fb90ff654746b))
* update lock file ([d888794](https://github.com/flowcore-io/data-pump/commit/d8887940f16fff7e3357651ceafbc5530f6045c6))

## [0.2.1](https://github.com/flowcore-io/data-pump/compare/v0.2.0...v0.2.1) (2025-02-20)


### Bug Fixes

* remove usage of console ([29f747c](https://github.com/flowcore-io/data-pump/commit/29f747c8ec0f41ab39766df9ef25a8b6fcf6fc91))

## [0.2.0](https://github.com/flowcore-io/data-pump/compare/v0.1.11...v0.2.0) (2025-02-20)


### Features

* remove time-uuid and cassandra driver util and use package ([3b35efa](https://github.com/flowcore-io/data-pump/commit/3b35efa912f430d6404ce30500b7582a2e200526))


### Bug Fixes

* logging changes ([5ac7fa3](https://github.com/flowcore-io/data-pump/commit/5ac7fa3daba5876ea2194c3bb87c2a5819c32159))

## [0.1.11](https://github.com/flowcore-io/data-pump/compare/v0.1.10...v0.1.11) (2025-02-19)


### Bug Fixes

* fix for stop at ([efded7a](https://github.com/flowcore-io/data-pump/commit/efded7ad473e9b533378eb0b0c629ca7805ba2e4))

## [0.1.10](https://github.com/flowcore-io/data-pump/compare/v0.1.9...v0.1.10) (2025-02-19)


### Bug Fixes

* add stop at option ([e0d90eb](https://github.com/flowcore-io/data-pump/commit/e0d90eb48dea096ee8cc9602b9b3acf9a706d7f8))

## [0.1.9](https://github.com/flowcore-io/data-pump/compare/v0.1.8...v0.1.9) (2025-02-19)


### Bug Fixes

* add getDate to time uuid util ([03d7d2e](https://github.com/flowcore-io/data-pump/commit/03d7d2eb8d621368aa08ade148dea7e4561e90e1))

## [0.1.8](https://github.com/flowcore-io/data-pump/compare/v0.1.7...v0.1.8) (2025-02-19)


### Bug Fixes

* add return types to uuid utils ([a0c56e8](https://github.com/flowcore-io/data-pump/commit/a0c56e8d0dd1c9bb27b61e6d27163fa8d16f414a))
* export time uuid util ([b08cc80](https://github.com/flowcore-io/data-pump/commit/b08cc80a9d7ae78dad027a8f370c7af9854acbf0))

## [0.1.7](https://github.com/flowcore-io/data-pump/compare/v0.1.6...v0.1.7) (2025-02-17)


### Bug Fixes

* rename to maxRedeliveryCount ([5ad7484](https://github.com/flowcore-io/data-pump/commit/5ad74848f55e60f37a4a29b40641d72ae9d8596c))

## [0.1.6](https://github.com/flowcore-io/data-pump/compare/v0.1.5...v0.1.6) (2025-02-17)


### Bug Fixes

* add onEventsFail ([00c59ff](https://github.com/flowcore-io/data-pump/commit/00c59ffae41dd64260e99bb439da553865bc5c5b))
* set onFailedEvents ([09f8015](https://github.com/flowcore-io/data-pump/commit/09f8015e2171c587f3e23edd44313cdebe1fcc4f))
* update readme ([b078763](https://github.com/flowcore-io/data-pump/commit/b078763a77994a701929500ac8c09fff1629b941))

## [0.1.5](https://github.com/flowcore-io/data-pump/compare/v0.1.4...v0.1.5) (2025-02-14)


### Bug Fixes

* add type ([a59707c](https://github.com/flowcore-io/data-pump/commit/a59707c52c735acb2c39c9a106698d609eb76a4a))

## [0.1.4](https://github.com/flowcore-io/data-pump/compare/v0.1.3...v0.1.4) (2025-02-14)


### Bug Fixes

* changes ([42426c5](https://github.com/flowcore-io/data-pump/commit/42426c5554fae40586fbd7508c52719d1bbe5808))
* fix type ([114d42e](https://github.com/flowcore-io/data-pump/commit/114d42e492de12c6af4c73c5469914bba8d4f7c3))
* update lock file ([0afeb02](https://github.com/flowcore-io/data-pump/commit/0afeb02c631e80c9fe9cd3bc80fb83d0a72dbbdb))

## [0.1.3](https://github.com/flowcore-io/data-pump/compare/v0.1.2...v0.1.3) (2025-02-13)


### Bug Fixes

* fix statemanager when buffer is empty ([9e6b5b6](https://github.com/flowcore-io/data-pump/commit/9e6b5b6f7806b2c294faceaaf2b49141e51231dc))

## [0.1.2](https://github.com/flowcore-io/data-pump/compare/v0.1.1...v0.1.2) (2025-02-12)


### Bug Fixes

* force version ([31bbc75](https://github.com/flowcore-io/data-pump/commit/31bbc75c9173bff7c3db18ecb244103e84e152e1))

## [0.1.1](https://github.com/flowcore-io/data-pump/compare/v0.1.0...v0.1.1) (2025-02-12)


### Bug Fixes

* new version ([15d41c9](https://github.com/flowcore-io/data-pump/commit/15d41c907a105d8b052ace3b8b6e5064b3f7c4e6))

## [0.1.0](https://github.com/flowcore-io/data-pump/compare/v0.0.1...v0.1.0) (2025-02-12)


### Features

* first version ([345e3ee](https://github.com/flowcore-io/data-pump/commit/345e3ee17067080a6f68cbb5819bfd99edfccdc1))


### Bug Fixes

* add declaration file for timeuuid ([1e98f13](https://github.com/flowcore-io/data-pump/commit/1e98f137a45e79b45fefbfb393bf58f772e293ce))
* add example test file ([1611ae8](https://github.com/flowcore-io/data-pump/commit/1611ae8e35a859c8aced6586c8addbec6d334f3c))
* update deno lock file ([9911fc2](https://github.com/flowcore-io/data-pump/commit/9911fc2645cd4c2e29dd8502e65feea4fccec6e7))
* update readme ([a8a1f22](https://github.com/flowcore-io/data-pump/commit/a8a1f229aeeb4742ce6da52fba29882376b3bd15))
* use alternative to cassandra uuid ([d13b293](https://github.com/flowcore-io/data-pump/commit/d13b293450e9d3f9db9f9f2466ea84b0ea5e40f1))
* version ([eae0481](https://github.com/flowcore-io/data-pump/commit/eae0481864963a5de0b18b5d44bbf5acd45609d4))
