package contract

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
)

// Decoding failures. Every DecodeStrict error is a *DecodeError wrapping
// ErrDecode; ErrEmptyBody and ErrTrailingData are reachable as causes.
var (
	// ErrDecode is the parent of every DecodeStrict failure.
	ErrDecode = errors.New("invalid JSON body")
	// ErrEmptyBody reports a missing body.
	ErrEmptyBody = errors.New("body is required")
	// ErrTrailingData reports content after the first JSON object.
	ErrTrailingData = errors.New("body must contain a single JSON object")
)

// DecodeError describes a malformed document in client terms, without
// exposing Go identifiers.
type DecodeError struct {
	// Message is safe to return to the client.
	Message string
	// Cause is the underlying decoder error.
	Cause error
}

// Error implements error.
func (e *DecodeError) Error() string { return ErrDecode.Error() + ": " + e.Message }

// Unwrap exposes ErrDecode and the cause to errors.Is / errors.As.
func (e *DecodeError) Unwrap() []error { return []error{ErrDecode, e.Cause} }

// DecodeStrict reads exactly one JSON object from r into dst: unknown fields
// are rejected and nothing may follow the object. Every failure wraps
// ErrDecode; the error text never names Go types.
func DecodeStrict(r io.Reader, dst any) error {
	dec := json.NewDecoder(r)
	dec.DisallowUnknownFields()
	if err := dec.Decode(dst); err != nil {
		return describe(err)
	}
	return trailing(dec.Decode(&struct{}{}))
}

// trailing interprets the result of decoding past the first object: EOF is
// the only success, a decoded token or a syntax error is trailing data, and
// any other failure (a body limit, a transport error) keeps its real cause so
// callers can still classify it.
func trailing(err error) error {
	switch {
	case errors.Is(err, io.EOF):
		return nil
	case err == nil, errors.As(err, new(*json.SyntaxError)), errors.As(err, new(*json.UnmarshalTypeError)):
		return &DecodeError{Message: ErrTrailingData.Error(), Cause: ErrTrailingData}
	}
	return &DecodeError{Message: ErrTrailingData.Error(), Cause: err}
}

// describe maps encoding/json errors to client-facing messages.
func describe(err error) error {
	var syntax *json.SyntaxError
	var typed *json.UnmarshalTypeError
	switch {
	case errors.Is(err, io.EOF):
		return &DecodeError{Message: ErrEmptyBody.Error(), Cause: ErrEmptyBody}
	case errors.Is(err, io.ErrUnexpectedEOF):
		return &DecodeError{Message: "malformed JSON: unexpected end of body", Cause: err}
	case errors.As(err, &syntax):
		return &DecodeError{Message: fmt.Sprintf("malformed JSON at offset %d", syntax.Offset), Cause: err}
	case errors.As(err, &typed):
		return &DecodeError{Message: typeMessage(typed), Cause: err}
	}
	if name, ok := strings.CutPrefix(err.Error(), "json: unknown field "); ok {
		return &DecodeError{Message: "unknown field " + name, Cause: err}
	}
	return &DecodeError{Message: "malformed JSON", Cause: err}
}

// typeMessage names the offending JSON field and the JSON type it expects.
func typeMessage(e *json.UnmarshalTypeError) string {
	field := e.Field
	if field == "" {
		field = "body"
	}
	return fmt.Sprintf("field %s must be a %s", field, jsonType(e.Type.Kind().String()))
}

// jsonType translates a Go kind into the JSON type the client must send.
func jsonType(kind string) string {
	switch kind {
	case "string":
		return "string"
	case "bool":
		return "boolean"
	case "struct", "map":
		return "object"
	case "slice", "array":
		return "array"
	default:
		return "number"
	}
}
