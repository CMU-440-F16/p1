package lsp

import (
	"encoding/json"

)

var MAX_MESSAGE_SIZE = 1000

func marShalMessage(msg *Message) []byte {
	marshaledMsg, _ := json.Marshal(msg)

	return marshaledMsg
}

func max (x,y int) int {
	if x >= y {
		return x
	} else {
		return y
	}
}
