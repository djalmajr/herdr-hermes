// Package a2ainterop holds the end-to-end interoperability tests of the
// A2A slice: the official A2A Go SDK client against the real Nau server
// (internal/a2aserver) on the loopback, over a real store
// (internal/a2astore) in a temp directory. Every subtest is hermetic:
// generated fixtures only, a bounded deadline on every wait, no real
// herdr-soho (the delivery sender is an in-memory fake).
package a2ainterop
