// Contains the implementation of a LSP client.

package lsp

import (
	"p1/src/github.com/cmu440/lspnet"

	"encoding/json"

	"fmt"
)

type client struct {
	debugMode bool
	connId    int
	udpConn   *lspnet.UDPConn
	seqNumber int
}

// NewClient creates, initiates, and returns a new client. This function
// should return after a connection with the server has been established
// (i.e., the client has received an Ack message from the server in response
// to its connection request), and should return a non-nil error if a
// connection could not be made (i.e., if after K epochs, the client still
// hasn't received an Ack message from the server in response to its K
// connection requests).
//
// hostport is a colon-separated string identifying the server's host address
// and port number (i.e., "localhost:9999").
func NewClient(hostport string, params *Params) (Client, error) {
	udpAddr, err := lspnet.ResolveUDPAddr("udp", hostport)
	if err != nil {
		return nil, err
	}

	udpConn, err := lspnet.DialUDP("udp", nil, udpAddr)
	if err != nil {
		return nil, err
	}

	client := client{debugMode: false, seqNumber: 0, udpConn: udpConn}
	if client.debugMode {
		fmt.Println("real udp conn ok")
	}

	connMessage := NewConnect()
	connByteMessage, _ := json.Marshal(connMessage)
	client.ConnectWrite(connByteMessage)
	if client.debugMode {
		fmt.Println("lsp conn message send")
	}
	ackMessage, err := client.Read()
	if client.debugMode {
		fmt.Println("lsp conn ack received")
	}
	if err != nil {
		// TODO 超时处理
	}
	var ack Message
	json.Unmarshal(ackMessage, &ack)

	client.connId = ack.ConnID
	client.seqNumber++

	return &client, nil
}

func (c *client) ConnID() int {
	return c.connId
}

func (c *client) Read() ([]byte, error) {
Here:
	buffer := make([]byte, MAX_MESSAGE_SIZE)

	len, err := c.udpConn.Read(buffer)
	if err != nil {

	}
	if c.debugMode {
		fmt.Println("client readLen:", len)
	}

	buffer = buffer[:len]
	var message Message
	json.Unmarshal(buffer, &message)
	if c.debugMode {
		fmt.Println("received message:", message)
	}
	// 判断是ACK还是普通的Message(client 不会收到connMessage)
	switch message.Type {
	case MsgData:
		if c.debugMode {
			fmt.Println("plain message")
			fmt.Println("plain message size", message.Size)
		}
		ack, _ := json.Marshal(NewAck(message.ConnID, message.SeqNum))
		ACKWrite(c.udpConn, nil, ack)
		return message.Payload, nil
	case MsgAck:
		if message.SeqNum == 0 {
			if c.debugMode {
				fmt.Println("conn ack message received")
			}
			// 对于conn的ACK 将其返回交给上层unmarshal处理
			return buffer[:len], nil
		} else {
			if c.debugMode {
				fmt.Println("message ack received")
			}
			c.seqNumber = message.SeqNum + 1
			goto Here
		}
	default:
		goto Here
	}

}

func (c *client) Write(payload []byte) error {
	// 由于此处的payload只是数据信息，需要封装包头
	dataMessage, _ := json.Marshal(NewData(c.connId, c.seqNumber, len(payload), payload))
	if c.debugMode {
		fmt.Println("message send:", string(payload))
	}

	_, err := c.udpConn.Write(dataMessage)
	return err
}

// 由于Write方法发送的是data message的payload
// 此处为了方便起见，将connect和Write分开，此处的connectMessage为Message类型marshal之后的结果
func (c *client) ConnectWrite(connectMessage []byte) {
	c.udpConn.Write(connectMessage)
}

func (c *client) Close() error {
	if c.debugMode {
		fmt.Println("client close")
	}
	return c.udpConn.Close()
}
