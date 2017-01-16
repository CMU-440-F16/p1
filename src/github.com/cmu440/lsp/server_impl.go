// Contains the implementation of a LSP server.

package lsp

import (
	"errors"

	"p1/src/github.com/cmu440/lspnet"

	"strconv"

	"encoding/json"
	"fmt"
)

type server struct {
	debugMode                 bool
	udpConn                   *lspnet.UDPConn
	clients                   map[int]*clientProxy // connID - client 的map
	nextConnID                int
	addrMap                   map[string]int // 判断重复的conn消息 使用addr(ip + port)区别，用于处理重复的conn消息
	serverReadMessageChannel  chan *Message
	clientConnMessageChannel  chan *lspnet.UDPAddr // 由于conn消息都是(conn,0,0)，此处采用发送UDPAddr区分
	clientAckMessageChannel   chan *Message
	serverWriteMessageChannel chan *Message // 如果同时write和read都修改sequence Number的话会出现race condition
}

type clientProxy struct {
	clientAddr *lspnet.UDPAddr
	seqNumber  int
}

// NewServer creates, initiates, and returns a new server. This function should
// NOT block. Instead, it should spawn one or more goroutines (to handle things
// like accepting incoming client connections, triggering epoch events at
// fixed intervals, synchronizing events using a for-select loop like you saw in
// project 0, etc.) and immediately return. It should return a non-nil error if
// there was an error resolving or listening on the specified port number.
func NewServer(port int, params *Params) (Server, error) {
	udpAddr, err := lspnet.ResolveUDPAddr("udp", ":"+strconv.Itoa(port))
	if err != nil {
		return nil, err
	}

	conn, err := lspnet.ListenUDP("udp", udpAddr)
	if err != nil {
		return nil, err
	}
	server := &server{false, conn, make(map[int]*clientProxy), 1, make(map[string]int), make(chan *Message), make(chan *lspnet.UDPAddr), make(chan *Message), make(chan *Message)}
	go server.ReadRoutine()
	go server.mainRoutine()
	return server, nil
}

func (s *server) mainRoutine() {
	for {
		select {
		case udpAddr := <-s.clientConnMessageChannel:
			connID, existed := s.addrMap[udpAddr.String()]
			if existed {
				// 已经存在了addr对应的connection
				// 重发(ack,connID,0)
				ackMessage, _ := json.Marshal(NewAck(connID, 0))
				ACKWrite(s.udpConn, udpAddr, ackMessage)
			} else {
				ackMessage, _ := json.Marshal(NewAck(s.nextConnID, 0))
				if s.debugMode {
					fmt.Println("server conn ack:", ackMessage)
				}
				s.clients[s.nextConnID] = &clientProxy{seqNumber: 1, clientAddr: udpAddr}
				s.addrMap[udpAddr.String()] = s.nextConnID
				s.nextConnID++
				ACKWrite(s.udpConn, udpAddr, ackMessage)
			}
		case ackMessage := <-s.clientAckMessageChannel:
			proxy, existed := s.clients[ackMessage.ConnID]
			if existed {
				proxy.seqNumber = ackMessage.SeqNum + 1
			}
		// 防止write和read的时候seqNumber的读写竞争，所以此处将read和write都放到mainRoutine处理
		case dataMessage := <-s.serverWriteMessageChannel:
			client := s.clients[dataMessage.ConnID]
			dataMessage.SeqNum = client.seqNumber
			messageSend, _ := json.Marshal(dataMessage)
			s.udpConn.WriteToUDP(messageSend, client.clientAddr)
		}
	}
}

// 由于udp无连接，采用一个readRoutine读取所有的client Message
// 包括conn ack data
func (s *server) ReadRoutine() {
	for {
		select {
		default:
			buffer := make([]byte, MAX_MESSAGE_SIZE)
			n, addr, _ := s.udpConn.ReadFromUDP(buffer)
			if s.debugMode {
				fmt.Println("server received message")
			}

			buffer = buffer[:n]

			var message Message

			json.Unmarshal(buffer, &message)

			switch message.Type {
			case MsgData: // 回ack并返回connID, payload

				ackMessage, _ := json.Marshal(NewAck(message.ConnID, message.SeqNum))
				ACKWrite(s.udpConn, addr, ackMessage)
				s.serverReadMessageChannel <- &message
			case MsgAck: // 修改对应client的seqNumber

				s.clientAckMessageChannel <- &message
			case MsgConnect:
				s.clientConnMessageChannel <- addr

			}
		}
	}
}

// 由于server的read方法可能由上层调用，因为serverMessageChannel从mainRoutine剥离
func (s *server) Read() (int, []byte, error) {
	dataMessage := <-s.serverReadMessageChannel

	return dataMessage.ConnID, dataMessage.Payload, nil
}

// 由于Write只是写date Message的payload
// server的ack部分由common.go的AckWrite完成
func (s *server) Write(connID int, payload []byte) error {
	_, ok := s.clients[connID]
	if !ok {
		return errors.New("client does not exist, connection may be lost or never established")
	}

	s.serverWriteMessageChannel <- NewData(connID, 0, len(payload), payload)
	/*	dataMessage, _ := json.Marshal(NewData(connID, client.seqNumber, len(payload), payload))
		_, e := s.udpConn.WriteToUDP(dataMessage, client.clientAddr)*/

	return nil
}

func (s *server) CloseConn(connID int) error {
	return errors.New("not yet implemented")
}

func (s *server) Close() error {
	return errors.New("not yet implemented")
}
