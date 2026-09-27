package coap

// MessageType is the 2-bit type field of a CoAP message.
type MessageType uint8

const (
	CON MessageType = iota // Confirmable
	NON                    // Non-confirmable
	ACK                    // Acknowledgement
	RST                    // Reset
)

// Method codes (class 0).
const (
	CodeEmpty uint8 = 0
	CodeGET   uint8 = 1
	CodePOST  uint8 = 2
	CodePUT   uint8 = 3
	CodeDEL   uint8 = 4
)

// Response codes (class 2..5), encoded as class<<5 | detail.
func ResponseCode(class, detail uint8) uint8 { return class<<5 | detail }

const (
	CodeCreated            uint8 = 65  // 2.01
	CodeDeleted            uint8 = 66  // 2.02
	CodeValid              uint8 = 67  // 2.03
	CodeChanged            uint8 = 68  // 2.04
	CodeContent            uint8 = 69  // 2.05
	CodeBadRequest         uint8 = 128 // 4.00
	CodeNotFound           uint8 = 132 // 4.04
	CodeMethodNotAllowed   uint8 = 133 // 4.05
	CodeRequestEntityLarge uint8 = 141 // 4.13
	CodeInternalError      uint8 = 160 // 5.00
)

// Option numbers used by this stack.
const (
	OptionIfMatch       uint16 = 1
	OptionURIHost       uint16 = 3
	OptionETag          uint16 = 4
	OptionIfNoneMatch   uint16 = 5
	OptionObserve       uint16 = 6
	OptionURIPort       uint16 = 7
	OptionLocationPath  uint16 = 8
	OptionURIPath       uint16 = 11
	OptionContentFormat uint16 = 12
	OptionMaxAge        uint16 = 14
	OptionURIQuery      uint16 = 15
	OptionAccept        uint16 = 17
	OptionLocationQuery uint16 = 20
	OptionBlock2        uint16 = 23
	OptionBlock1        uint16 = 27
	OptionSize2         uint16 = 28
	OptionSize1         uint16 = 60
)

// MaxTokenLen is the maximum token length (TKL field is 4 bits, RFC 7252 caps at 8).
const MaxTokenLen = 8
