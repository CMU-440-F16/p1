package lsp

import (
	"encoding/json"
)

var MAX_MESSAGE_SIZE = 1000

func marshalMessage(msg *Message) []byte {
	marshaledMsg, _ := json.Marshal(msg)

	return marshaledMsg
}

func unmarshalMessage(msg []byte) *Message {
	var message Message
	json.Unmarshal(msg, &message)

	return &message
}

func max(x, y int) int {
	if x >= y {
		return x
	} else {
		return y
	}
}

func dataMsgSizeVA(dataMsg *Message) bool {
	if len(dataMsg.Payload) < dataMsg.Size {
		return false
	} else {
		dataMsg.Payload = dataMsg.Payload[:dataMsg.Size]
		return true
	}
}
