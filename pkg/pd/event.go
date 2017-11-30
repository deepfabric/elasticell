// Copyright 2016 DeepFabric, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package pd

var (
	// EventInit event init
	EventInit = uint32(1 << 1)
	// EventCellCreated event cell created
	EventCellCreated = uint32(1 << 2)
	// EventCellLeaderChanged event cell created
	EventCellLeaderChanged = uint32(1 << 3)
	// EventCellRangeChaned event cell range changed
	EventCellRangeChaned = uint32(1 << 4)
	// EventCellPeersChaned event cell peer changed
	EventCellPeersChaned = uint32(1 << 5)
	// EventStoreUp event store status was up
	EventStoreUp = uint32(1 << 6)
	// EventStoreDown event store status was down
	EventStoreDown = uint32(1 << 7)
	// EventStoreTombstone event store status was tombstone
	EventStoreTombstone = uint32(1 << 8)

	// EventFlagCell all cell event
	EventFlagCell = uint32(EventCellCreated | EventCellLeaderChanged | EventCellRangeChaned | EventCellPeersChaned)
	// EventFlagStore all store event
	EventFlagStore = uint32(EventStoreUp | EventStoreDown | EventStoreTombstone)
	// EventFlagAll all event
	EventFlagAll = uint32(0xffffffff)
)

// MatchEvent returns the flag has the target event
func MatchEvent(event, flag uint32) bool {
	return event == 0 || event&flag != 0
}
