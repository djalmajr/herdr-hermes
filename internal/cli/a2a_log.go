package cli

// The a2a serve logger: the JSON handler the a2a serve puts on stderr,
// at the WARN level, with every URL of a record masked before it is
// written. A webhook URL can carry a secret in the path or the query,
// and a log record must never carry it.

import (
	"io"
	"log/slog"
	"regexp"
)

// a2aURLPattern is a URL in a log value: a scheme, "://" and the
// characters up to a space, a quote or the end.
var a2aURLPattern = regexp.MustCompile(`[A-Za-z][A-Za-z0-9+.-]*://[^\s"]*`)

// a2aURLMask is the fixed replacement of every URL in a log value.
const a2aURLMask = "<url>"

// a2aMaskURLs replaces every URL of the value by the fixed token <url>.
func a2aMaskURLs(value string) string {
	return a2aURLPattern.ReplaceAllString(value, a2aURLMask)
}

// a2aServeLogger builds the logger of the a2a serve: a JSON handler on
// the given writer at the WARN level whose ReplaceAttr masks every URL
// of the message and of every string or error attribute (an error
// becomes the string of the already-masked error); the other values
// are untouched.
func a2aServeLogger(w io.Writer) *slog.Logger {
	return slog.New(slog.NewJSONHandler(w, &slog.HandlerOptions{
		Level:       slog.LevelWarn,
		ReplaceAttr: a2aServeMaskAttr,
	}))
}

// a2aServeMaskAttr implements the ReplaceAttr of the a2a serve logger:
// the message and every attribute whose value is a string or an error
// are written with every URL masked as <url>; the other values are
// untouched.
func a2aServeMaskAttr(_ []string, a slog.Attr) slog.Attr {
	switch v := a.Value.Any().(type) {
	case string:
		a.Value = slog.StringValue(a2aMaskURLs(v))
	case error:
		a.Value = slog.StringValue(a2aMaskURLs(v.Error()))
	}
	return a
}
