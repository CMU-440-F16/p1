package lsp

import (
	"encoding/json"
	"p1/src/github.com/cmu440/lspnet"
)

var MAX_MESSAGE_SIZE = 1000

func ACKWrite(conn *lspnet.UDPConn, clientAddr *lspnet.UDPAddr, ackMessage []byte) {
	if clientAddr == nil {
		conn.Write(ackMessage)
	} else {
		conn.WriteToUDP(ackMessage, clientAddr)
	}
}

func marShalMessage(msg *Message) []byte {
	marshaledMsg, _ := json.Marshal(msg)

	return marshaledMsg
}
