package lsp

import "p1/src/github.com/cmu440/lspnet"

var MAX_MESSAGE_SIZE = 1000

func ACKWrite(conn *lspnet.UDPConn, clientAddr *lspnet.UDPAddr, ackMessage []byte) {
	if clientAddr == nil {
		conn.Write(ackMessage)
	} else {
		conn.WriteToUDP(ackMessage, clientAddr)
	}
}
