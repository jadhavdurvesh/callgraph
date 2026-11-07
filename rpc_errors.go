package callgraph

import (
	"encoding/binary"
	"errors"
	"fmt"
)

const (
	ErrorCodeMethodNotFound uint16 = 1001
	ErrorCodeOperationVersion uint16 = 1002
	ErrorCodeUnauthorized uint16 = 1003
	ErrorCodeDeadlineExceeded uint16 = 1004
	ErrorCodePayloadLimit uint16 = 1005
	ErrorCodeInternal uint16 = 1500
)

var ErrMalformedErrorPayload = errors.New("callgraph: malformed structured error payload")

type RemoteError struct {
	Code uint16
	Message string
}
func (e *RemoteError) Error() string { return fmt.Sprintf("callgraph: remote error %d: %s", e.Code, e.Message) }
func (e *RemoteError) Is(target error) bool { return target == ErrRemoteOperation }

func encodeErrorPayload(code uint16, message string) []byte {
	if len(message) > 4096 { message = message[:4096] }
	out := make([]byte, 8+len(message))
	copy(out[:4], "ERR1")
	binary.BigEndian.PutUint16(out[4:6], code)
	binary.BigEndian.PutUint16(out[6:8], uint16(len(message)))
	copy(out[8:], message)
	return out
}

func DecodeErrorPayload(payload []byte) (uint16, string, error) {
	if len(payload) < 8 || string(payload[:4]) != "ERR1" {
		return 0, "", ErrMalformedErrorPayload
	}
	n := int(binary.BigEndian.Uint16(payload[6:8]))
	if n != len(payload)-8 { return 0, "", ErrMalformedErrorPayload }
	return binary.BigEndian.Uint16(payload[4:6]), string(payload[8:]), nil
}
