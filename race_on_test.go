//go:build race

package main

// The race detector adds enough overhead to make a latency budget
// meaningless, so the measurement skips itself under `go test -race`.
const raceDetectorEnabled = true
