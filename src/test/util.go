package test

import "testing"

// SkipIfShort skips a test if testing in short mode
func SkipIfShort(t *testing.T) {
	if testing.Short() {
		// t.Skip() kills the goroutine
		t.Skip("Skipping " + t.Name() + " since it's not a unit test.")
	}
}
