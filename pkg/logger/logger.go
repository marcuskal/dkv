// Package logger provides a structured logging facade for dkv.
// Wrapping zerolog behind this package means swapping the backend
// (e.g. to zap) is a one-file change.
package logger

import (
	"io"
	"os"
	"time"

	"github.com/rs/zerolog"
)

// New returns a zerolog.Logger configured for the given level and output.
// In production you'd typically write JSON to stdout and let the sidecar
// (fluentbit, vector) ship it to your log aggregator.
func New(level string, w io.Writer) zerolog.Logger {
	if w == nil {
		w = os.Stdout
	}

	lvl, err := zerolog.ParseLevel(level)
	if err != nil {
		lvl = zerolog.InfoLevel
	}

	return zerolog.New(w).
		Level(lvl).
		With().
		Timestamp().
		Caller(). // includes file:line — invaluable during incident triage
		Logger().
		Output(zerolog.ConsoleWriter{
			Out:        w,
			TimeFormat: time.RFC3339,
		})
}

// NewJSON returns a pure JSON logger (no pretty-printing).
// Use this in production; the ConsoleWriter above is for local dev.
func NewJSON(level string, w io.Writer) zerolog.Logger {
	if w == nil {
		w = os.Stdout
	}

	lvl, err := zerolog.ParseLevel(level)
	if err != nil {
		lvl = zerolog.InfoLevel
	}

	return zerolog.New(w).
		Level(lvl).
		With().
		Timestamp().
		Caller().
		Logger()
}