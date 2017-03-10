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

package util

// GetIntValue get int value
// if value if 0 reutrn default value
func GetIntValue(value, defaultValue int) int {
	if value == 0 {
		return defaultValue
	}

	return value
}

// GetUint64Value get uint64 value
// if value if 0 reutrn default value
func GetUint64Value(value, defaultValue uint64) uint64 {
	if value == 0 {
		return defaultValue
	}

	return value
}

// GetStringValue get string value
// if value if "" reutrn default value
func GetStringValue(value, defaultValue string) string {
	if value == "" {
		return defaultValue
	}

	return value
}
