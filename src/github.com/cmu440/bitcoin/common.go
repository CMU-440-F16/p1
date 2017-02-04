package bitcoin

import (
	"encoding/json"
)

func MarshalMessage(msg *Message) []byte {
	marshaledMsg, _ := json.Marshal(msg)

	return marshaledMsg
}

func UnmarshalMessage(msg []byte) *Message {
	var message Message
	json.Unmarshal(msg, &message)

	return &message
}
