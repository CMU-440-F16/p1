// Contains the implementation of a LSP server.

package lsp

import (
	"errors"

	"p1/src/github.com/cmu440/lspnet"

	"strconv"

	"container/list"
	"fmt"
	"time"
)

type server struct {
	debugMode  bool
	udpConn    *lspnet.UDPConn
	clients    map[int]*clientProxy // connID - client map
	nextConnID int
	addrMap    map[string]int // 判断重复的conn消息 使用addr(ip + port)区别，用于处理重复的conn消息

	epochLimit    int
	epochTicker   *time.Ticker
	windowSize    int
	readReq       bool
	closed        bool
	closedSuccess bool

	readBuffer *list.List

	serverDataMessageChannel  chan *Message        // 用于read方法
	serverConnMessageChannel  chan *lspnet.UDPAddr // 由于conn消息都是(conn,0,0)，此处采用发送UDPAddr区分
	serverAckMessageChannel   chan *Message        // 用于main routine的处理
	serverWriteMessageChannel chan *Message        // 如果同时write和read都修改sequence Number的话会出现race condition

	serverReadRequest  chan int      // client的read请求
	serverReadResponse chan *Message // read请求的返回

	serverCloseClientChannel chan int

	serverCloseChannel         chan int
	serverCloseResponseChannel chan int

	serverReadRoutineExitChannel chan int
	serverMainRoutineExitChannel chan int
}

type clientProxy struct {
	clientAddr *lspnet.UDPAddr
	sender     *Sender
	//sendSeqNumber int // 和ack read, msg write有关 初始为1 ，为每一个发送给client的包进行编号
	readSeqNumber int // 和msg read, ack write(epoch)有关 初始为1(server端的clientProxy只需要从1开始读dataMsg，下同), 代表下一个接受的消息的序号(小于该序号的dataMessage的丢弃)
	serverReadSeq int // 下一个待加入server readBuffer的data seq 初始为1

	/*windowSize            int
	windowStart           int // client发送包的起始编号（没有ack,由于没有conn, start从1开始）
	windowEnd             int // client发送包的终止编号
	maxACKReq             int*/
	mostRecentReceivedSeq int  // 最近收到的dataMsg的seqNumber
	epochReceived         bool // 标记epoch是否收到了任何数据
	noMsgEpochs           int
	connLost              bool
	explicitClose         bool

	//sendBuffer    *list.List       // 缓存已经发送但是没有ack的数据或者是将要发送的数据(ack之后的消息从buffer中移除)
	receiveBuffer map[int]*Message // 缓存接收到，但是没有被上层read的数据(被上层读取后从buffer中移除)
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
	server := &server{false, conn, make(map[int]*clientProxy), 1, make(map[string]int), params.EpochLimit, time.NewTicker(time.Millisecond * time.Duration(params.EpochMillis)), params.WindowSize,
		false, false, true, list.New(), make(chan *Message), make(chan *lspnet.UDPAddr), make(chan *Message), make(chan *Message), make(chan int), make(chan *Message), make(chan int),
		make(chan int), make(chan int), make(chan int), make(chan int)}
	go server.readRoutine()
	go server.mainRoutine()
	//fmt.Println("server up")
	return server, nil
}

// 由于udp无连接，采用一个readRoutine读取所有的client Message
// 包括conn ack data
func (s *server) readRoutine() {
	for {
		select {
		case <-s.serverReadRoutineExitChannel:
			return
		default:
			buffer := make([]byte, MAX_MESSAGE_SIZE)
			n, addr, _ := s.udpConn.ReadFromUDP(buffer)

			buffer = buffer[:n]

			message := UnmarshalMessage(buffer)

			switch message.Type {
			case MsgData:
				//fmt.Println("server recv plain message:", message.String())
				if s.debugMode {
					fmt.Println("server received data message: ", message)
				}
				s.serverWriteMessageChannel <- NewAck(message.ConnID, message.SeqNum)
				s.serverDataMessageChannel <- message
			case MsgAck: // 修改对应client的seqNumber
				//fmt.Println("server recv ack message:", message.String())
				s.serverAckMessageChannel <- message
			case MsgConnect:
				//fmt.Println("server recv conn message:", message.String())
				s.serverConnMessageChannel <- addr
			}
		}
	}
}

func (s *server) mainRoutine() {
	for {
		select {
		case <-s.epochTicker.C: // epoch事件，对server的每一个clientProxy执行
			if s.closed && len(s.clients) == 0 {
				if s.closedSuccess {
					s.serverCloseResponseChannel <- 1
				} else {
					s.serverCloseResponseChannel <- 0
				}
				break
			}

			for i := 1; i < s.nextConnID; i++ { // i为connId
				c, existed := s.clients[i]
				if existed {
					if !c.epochReceived {
						c.noMsgEpochs++
					} else {
						c.noMsgEpochs = 0
						c.epochReceived = false
					}
					// 该client的connLost
					if c.noMsgEpochs >= s.epochLimit {

						c.connLost = true
						// 如果没有close, 则将pending data msg加入s.readBuffer
						if !c.explicitClose {
							for size := len(c.receiveBuffer); size > 0; {
								msg, ok := c.receiveBuffer[c.serverReadSeq]
								if ok {
									s.readBuffer.PushBack(msg)
									delete(c.receiveBuffer, c.serverReadSeq)
									size--
								}
								c.serverReadSeq++
							}
						}
						// 无论connLost还是conn close 都要加入一个代表err的dataMsg
						s.readBuffer.PushBack(NewData(i, 0, 0, nil))

						if s.readReq {
							s.readReq = false
							s.serverReadResponse <- s.readBuffer.Remove(s.readBuffer.Front()).(*Message)
						}

						// 某个client的connLost之后 删除server端的client
						delete(s.clients, i)
					} else {
						// 只有未close或者close之后的data没有全部ack的client才需要发送心跳(可能在全部消息都ack之后调用close时发生)
						if !c.explicitClose || c.sender.getBufferSize() > 0 {
							c.sender.SendAndBufferMsg(NewAck(i, c.mostRecentReceivedSeq), false)
							c.sender.SendWindowDataMsgInRange(nil, c.sender.getWindowStart(), c.sender.getWindowEnd())
						} else {
							delete(s.clients, i) // 删除client
						}
					}
				}
			}
		case udpAddr := <-s.serverConnMessageChannel:
			if !s.closed {
				connID, existed := s.addrMap[udpAddr.String()]
				if existed {
					Send(s.udpConn, NewAck(connID, 0), udpAddr)
				} else {
					s.clients[s.nextConnID] = &clientProxy{udpAddr, NewSender(s.udpConn, udpAddr, 1, s.windowSize, 1), 1, 1, 0, false, 0, false, false, make(map[int]*Message)}
					s.addrMap[udpAddr.String()] = s.nextConnID
					// 由于不能routine内部的channel互相发送，此处的ack直接写(发送到serverWrite会死锁)
					Send(s.udpConn, NewAck(s.nextConnID, 0), udpAddr)
					s.nextConnID++
				}
			}
		case ackMessage := <-s.serverAckMessageChannel:
			c, existed := s.clients[ackMessage.ConnID]
			if existed && !c.connLost {
				c.epochReceived = true
				c.sender.UpdateSendBuffer(ackMessage.SeqNum)

				if c.explicitClose && c.sender.getBufferSize() == 0 {
					//fmt.Println("client:", ackMessage.ConnID, "all msg ack, ready to exit")
					delete(s.clients, ackMessage.ConnID)
				}
			}
		case dataMessage := <-s.serverDataMessageChannel:
			if !DataMsgSizeVA(dataMessage) {
				break
			}
			// 防止write和read的时候seqNumber的读写竞争，所以此处将read和write都放到mainRoutine处理
			if s.debugMode {
				fmt.Println("server read msg", dataMessage)
			}
			if !s.closed { // close之后不再调用read()方法 因此受到的dataMsg直接忽略
				c, existed := s.clients[dataMessage.ConnID]
				if existed && !c.connLost {
					c.epochReceived = true
					// 更新readSeqNumber、receive buffer等信息
					// 如果有readReq,则返回对应的信息 并更新readReq和clientReadNumber
					c.mostRecentReceivedSeq = dataMessage.SeqNum
					if _, ok := c.receiveBuffer[dataMessage.SeqNum]; dataMessage.SeqNum >= c.readSeqNumber && !ok { // 重复的信息(小于client期望接受的seqNumber或者buffer中已经含有)丢弃

						c.receiveBuffer[dataMessage.SeqNum] = dataMessage
						// 如果是下一个期望的dataMsg seqNumber 则将client中连续的seqNumber的message都放入server的读取缓存中,并更新readSeq
						if dataMessage.SeqNum == c.readSeqNumber {
							s.readBuffer.PushBack(dataMessage)
							delete(c.receiveBuffer, c.readSeqNumber)
							c.readSeqNumber++
							c.serverReadSeq++
							// 将readSeqNumber更新到下一个没有收到的dataMsg seqNum
							for msg, ok := c.receiveBuffer[c.readSeqNumber]; ok; msg, ok = c.receiveBuffer[c.readSeqNumber] {
								s.readBuffer.PushBack(msg)
								delete(c.receiveBuffer, c.readSeqNumber)
								c.readSeqNumber++
								c.serverReadSeq++
							}
						}

						if s.readBuffer.Len() > 0 && s.readReq {
							s.readReq = false
							s.serverReadResponse <- s.readBuffer.Remove(s.readBuffer.Front()).(*Message)
						}
					}
				}
			}

		case writeMessage := <-s.serverWriteMessageChannel:

			c, existed := s.clients[writeMessage.ConnID]
			// ack直接发送
			if existed && !c.connLost {
				c.sender.SendAndBufferMsg(writeMessage, true)

			}
		case <-s.serverReadRequest:
			if s.readBuffer.Len() > 0 {
				s.serverReadResponse <- s.readBuffer.Remove(s.readBuffer.Front()).(*Message)
			} else {
				s.readReq = true
			}
		case connID := <-s.serverCloseClientChannel:
			c := s.clients[connID]
			c.explicitClose = true
			//fmt.Println("server close client:", connID)
			// 某个client显示关闭(非server.close())的话 需要向readBuffer或者response channel写入err信息
			if s.readReq {
				s.readReq = false
				s.serverReadResponse <- NewData(connID, 0, 0, nil)
			} else {
				s.readBuffer.PushBack(NewData(connID, 0, 0, nil))
			}

		case <-s.serverCloseChannel:
			s.closed = true

			for i := 1; i < s.nextConnID; i++ {
				c, existed := s.clients[i]
				if existed {
					// 调用close()方法之后不会有read() 所以不需要向readBuffer写入信息
					c.explicitClose = true
					if c.sender.getBufferSize() == 0 { // 可以删除client
						delete(s.clients, i)
					}
				}
			}
		case <-s.serverMainRoutineExitChannel:
			return
		}
	}
}

func (s *server) Read() (int, []byte, error) {
	if s.closed {
		return 0, nil, errors.New("server close")
	}
	s.serverReadRequest <- 0

	dataMessage := <-s.serverReadResponse
	// server的seqNum为0代表err信息(connID需要作为err时的返回值)
	if dataMessage.SeqNum == 0 {
		return dataMessage.ConnID, nil, errors.New("read err")
	} else {
		return dataMessage.ConnID, dataMessage.Payload, nil
	}
}

func (s *server) Write(connID int, payload []byte) error {
	c, ok := s.clients[connID]
	if !ok || c.connLost {
		return errors.New("conn does not exist or lost")
	} else {
		// 让mainRoutine写入seqNum 防止竞争条件
		s.serverWriteMessageChannel <- NewData(connID, 0, len(payload), payload)
		return nil
	}
}

func (s *server) CloseConn(connID int) error {
	c, ok := s.clients[connID]
	if !ok || c.connLost {
		return errors.New("conn does not exist or lost")
	}
	s.serverCloseClientChannel <- connID

	return nil
}

func (s *server) Close() error {
	//fmt.Println("mark close")
	s.serverCloseChannel <- 1

	res := <-s.serverCloseResponseChannel
	//fmt.Println("server main ready to exit:", res)

	// 同client的Close()，防止read routine在udpconn.read()处阻塞
	s.udpConn.Close()
	s.serverReadRoutineExitChannel <- 0
	//fmt.Println("server read routine exit:", res)
	s.serverMainRoutineExitChannel <- 0
	//fmt.Println("server main routine exit:", res)

	if res == 1 {
		return nil
	} else {
		return errors.New("server exit error due to some conn lost")
	}
}
