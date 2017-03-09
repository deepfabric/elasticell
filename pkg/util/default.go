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