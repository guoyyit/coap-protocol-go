package coap

import "errors"

var (
	// ErrInvalidVersion: message version field is not 1.
	ErrInvalidVersion = errors.New("coap: invalid version")
	// ErrMessageTruncated: datagram shorter than header or announced lengths.
	ErrMessageTruncated = errors.New("coap: message truncated")
	// ErrInvalidTokenLen: TKL > 8 or token overruns the datagram.
	ErrInvalidTokenLen = errors.New("coap: invalid token length")
	// ErrInvalidOptionDelta: option delta/length uses reserved value 15.
	ErrInvalidOptionDelta = errors.New("coap: invalid option delta/length")
	// ErrOptionTruncated: option header or value overruns the datagram.
	ErrOptionTruncated = errors.New("coap: option truncated")
	// ErrPayloadMarker: payload marker present but no payload follows.
	ErrPayloadMarker = errors.New("coap: payload marker without payload")
	// ErrMessageTooShort: datagram shorter than the 4-byte header.
	ErrMessageTooShort = errors.New("coap: message too short")
	// ErrTimeout: confirmable request exhausted retransmissions.
	ErrTimeout = errors.New("coap: request timed out")
	// ErrReset: peer rejected the message with RST.
	ErrReset = errors.New("coap: message reset by peer")
	// ErrNotObserving: no active observation for the token.
	ErrNotObserving = errors.New("coap: not observing")
	// ErrBlockTransfer: block-wise transfer failed or was aborted.
	ErrBlockTransfer = errors.New("coap: block-wise transfer failed")
	// ErrClosed: endpoint was closed.
	ErrClosed = errors.New("coap: endpoint closed")
)
