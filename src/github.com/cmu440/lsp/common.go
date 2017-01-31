package lsp

import (
	"encoding/json"
	"p1/src/github.com/cmu440/lspnet"
)

var MAX_MESSAGE_SIZE = 1000

func MarshalMessage(msg *Message) []byte {
	marshaledMsg, _ := json.Marshal(msg)

	return marshaledMsg
}

func Send(conn *lspnet.UDPConn, msg *Message, addr *lspnet.UDPAddr) {
	if addr != nil {
		conn.WriteToUDP(MarshalMessage(msg), addr)
	} else {
		conn.Write(MarshalMessage(msg))
	}
}

func UnmarshalMessage(msg []byte) *Message {
	var message Message
	json.Unmarshal(msg, &message)

	return &message
}

func Max(x, y int) int {
	if x >= y {
		return x
	} else {
		return y
	}
}

// payload过小则丢弃，过大则截断
func DataMsgSizeVA(dataMsg *Message) bool {
	if len(dataMsg.Payload) < dataMsg.Size {
		return false
	} else {
		dataMsg.Payload = dataMsg.Payload[:dataMsg.Size]
		return true
	}
}
