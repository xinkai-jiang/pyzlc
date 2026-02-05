# Changelog

All notable changes to this project will be documented in this file.

## [2.0.3] - 2026-02-05

### Changed
- Remove the same name publisher error since zlc allows duplicate publisher names

### Fixed
- Resolve deadlock and improve error handling

## [2.0.2] - 2026-01-29

### Added
- Add `submit_thread_pool_task` method to LanComLoopManager for submitting synchronous functions to the thread pool executor

### Fixed
- Fix bug in `handle_heartbeat` function

## [2.0.1] - 2026-01-28

### Added
- Create a short heartbeat message
- Add more functions to `__all__` exports

### Changed
- Change the init argument
- Add group argument to init function

## [1.1.3] - 2026-01-25

### Fixed
- Fix the bug of node unregister
- Fix the bugs of received node info from other net interface

## [1.1.2]

### Fixed
- Fix call issue and add timeout parameter

### Changed
- Add TypedDict to MessageT and re-raise keyboard exception

## [1.1.1]

### Changed
- Update description
- Auto publish to PyPI

## [1.1.0]

### Added
- Check response status
- Add test node info benchmark
- TaskReturnT for submit_loop_task
- Empty type for callback function
- Heartbeat and custom message example

### Changed
- Make it as modern Python repository
- Update README

## [1.0.1]

### Fixed
- Fix LanComLoopManager starts for twice and reuse the multicast port issue
- Clean publisher and fix service manager

### Changed
- Align to zerolancom
- Update service client
- Test publisher

## [1.0.0]

### Added
- Service manager and update asyncio loop
- Update the abstract class

### Fixed
- Fix the bug of running in the localhost
- Fix the bug of using on Windows
- Fix bugs of using msgpack to serialize/deserialize

### Changed
- Ready for change the name
- Test discover

## [0.1.0] - Initial Release

### Added
- Initial implementation of pyzlc
- LanComNode base implementation
- Master node functionality
- Broadcast and multicast support
- Service and proxy functionality
- Multi-part send for publishing messages with topic
- Auto subscription feature
- Register subscriber for previously launched nodes
- Auto subscribe to new topics
- Node socket by TCP
- Base data structure

### Fixed
- Fix the problem of timeout
- Fix reload master bugs
- Fix the bug of auto subscription
