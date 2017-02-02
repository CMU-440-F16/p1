// Contains the implementation of a LSP client.

package lsp

import (
	"p1/src/github.com/cmu440/lspnet"

	"errors"
	"fmt"
	"strconv"
	"time"
)

type client struct {
	debugMode bool
	connId    int
	udpConn   *lspnet.UDPConn
	sender    *Sender
	receiver  *Receiver
	reader    *Reader

	epochLimit            int
	epochTicker           *time.Ticker
	mostRecentReceivedSeq int  // 最近收到的dataMsg的seqNumber
	epochReceived         bool // 标记epoch是否收到了任何数据
	noMsgEpochs           int
	connLost              bool
	explicitClose         bool


	clientDataMessageChannel  chan *Message
	clientAckMessageChannel   chan *Message
	clientWriteMessageChannel chan *Message // 如果同时write和read都修改sequence Number的话会出现race condition
	clientReadRequest         chan int      // client的read请求
	clientReadResponse        chan *Message // read请求的返回

	explicitCloseSetChannel     chan int
	mainRoutineReadyExitChannel chan int
	mainRoutineExitChannel      chan int
	readRoutineExitChannel      chan int
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
		fmt.Println("cannot conn to server")
		return nil, err
	}

	client := client{false, 0, udpConn, NewSender(udpConn, nil, 0, params.WindowSize, 0), NewReceiver(0),  nil,params.EpochLimit, time.NewTicker(time.Millisecond * time.Duration(params.EpochMillis)),
		0, false, 0, false, false, make(chan *Message), make(chan *Message), make(chan *Message), make(chan int), make(chan *Message),
		make(chan int), make(chan int), make(chan int), make(chan int)}
	client.reader = NewReader(client.clientReadResponse)

	if client.debugMode {
		fmt.Println("real udp conn ok")
	}

	go client.mainRoutine()
	go client.readRoutine()
	client.clientWriteMessageChannel <- NewConnect()

	if client.debugMode {
		fmt.Println("lsp conn message send")
	}

	connID, err := client.Read()
	if client.debugMode {
		fmt.Println("lsp conn ack received")
	}
	// 无法建立连接则返回err
	if err != nil {
		fmt.Println(err)
		return nil, errors.New("can't establish conn with " + string(params.EpochLimit) + " time out")
	} else {
		client.connId, _ = strconv.Atoi(string(connID))
		return &client, nil
	}
}

func (c *client) readRoutine() {
	for {
		select {
		case <-c.readRoutineExitChannel:
			//fmt.Println("client read routing exit !!!!!!")
			return
		default:

			buffer := make([]byte, MAX_MESSAGE_SIZE)

			len, err := c.udpConn.Read(buffer)
			if err != nil {
				//fmt.Println("client read err")
				if c.debugMode {
					fmt.Println("readRoutine read err,", err)
				}
			} else {
				buffer = buffer[:len]
				message := UnmarshalMessage(buffer)
				if c.debugMode {
					fmt.Println("client received message:", message)
				}
				// 判断是ACK还是普通的Message(client 不会收到connMessage)
				switch message.Type {
				case MsgData:
					//fmt.Println("client recv plain message:", message.String())
					if c.debugMode {
						fmt.Println("plain message")
						fmt.Println("plain message size", message.Size)
					}
					c.clientWriteMessageChannel <- NewAck(c.connId, message.SeqNum)
					c.clientDataMessageChannel <- message

				case MsgAck:
					//fmt.Println("client recv ack message:", message.String())
					if c.debugMode {
						fmt.Println("ack message received")
					}
					c.clientAckMessageChannel <- message
				}
			}
		}
	}
}

func (c *client) mainRoutine() {
	for {
		select {
		case <-c.epochTicker.C:
			if !c.epochReceived {
				c.noMsgEpochs++
			} else {
				c.epochReceived = false
				c.noMsgEpochs = 0
			}

			if c.noMsgEpochs >= c.epochLimit {
				c.connLost = true

				if c.explicitClose { // close阶段发生lost 直接return
					c.epochTicker.Stop() // 当lost且close之后才Stop定时器
					c.mainRoutineReadyExitChannel <- 0
				} else {

					c.receiver.movePendingMsgToReader(c.reader)
					// 表示连接已经断开的消息
					c.reader.OfferMsgWithReqCheck(NewErrMsg(0))
				}

			} else {
				// epoch事件
				// 1、conn无ack的话resend conn request
				// 2、建立了连接但是没有从server接到过data,发送seq为0的ack
				// 3、从server接收到过data,则发送最近接收数据的ack
				// 4、resend未ack的数据
				if c.connId == 0 {
					// 情况1 发送Conn，此时肯定没有未ack的dataMsg
					//fmt.Println("client write conn in epoch")
					c.sender.SendWindowDataMsgInRange(nil, c.sender.getWindowStart(), c.sender.getWindowEnd())
				} else {
					// 最新的dataMsg的ack消息，不缓存
					c.sender.SendAndBufferMsg(NewAck(c.connId, c.mostRecentReceivedSeq), false)
					// 发送所有在sendBuffer [windosStart, windowEnd]中的数据
					c.sender.SendWindowDataMsgInRange(nil, c.sender.getWindowStart(), c.sender.getWindowEnd())
				}
			}

		case ackMessage := <-c.clientAckMessageChannel:
			if c.debugMode {
				fmt.Println("main routine ack")
			}
			// close之后还是需要接收ack消息
			if !c.connLost {

				// 更新send buffer、如果刚好是windowStart的话更新window start和window end
				c.epochReceived = true
				c.sender.UpdateSendBuffer(ackMessage.SeqNum)

				// conn的ack消息(防止重复接受server端发送的conn ack作为上层read的结果)
				if ackMessage.SeqNum == 0 && c.connId == 0 {
					// seqNumber为0的ack含有connId，需要缓存，为了和dataMsg一起处理，此处将payload设置为connId的byte数组
					ackMessage.Payload = []byte(strconv.Itoa(ackMessage.ConnID))
					c.receiver.BufferRecvMsgAndUpDate(ackMessage, c.reader)
					// client尝试了read()，则返回并更新
				}

				if c.explicitClose && c.sender.getBufferSize() == 0 {
					//fmt.Println("client finish all ack exit")
					c.epochTicker.Stop()
					c.mainRoutineReadyExitChannel <- 0
				}

			}

		case dataMessage := <-c.clientDataMessageChannel:
			if !DataMsgSizeVA(dataMessage) {
				break
			}
			if c.debugMode {
				fmt.Println("main routine data read")
			}
			// conn丢失或者close的话不接收server的数据
			if !c.connLost && !c.explicitClose {
				c.epochReceived = true
				// 更新readSeqNumber、receive buffer等信息
				// 如果有readReq,则返回对应的信息 并更新readReq和clientReadNumber
				c.mostRecentReceivedSeq = dataMessage.SeqNum

				c.receiver.BufferRecvMsgAndUpDate(dataMessage, c.reader)

			}

		case writeMessage := <-c.clientWriteMessageChannel:
			if c.debugMode {
				fmt.Println("main routine write msg:", writeMessage)
			}
			// 初始的Conn和dataMsg需要缓存
			c.sender.SendAndBufferMsg(writeMessage, true)

		case <-c.clientReadRequest: // 标记收到了上方的read请求, 可能是读取消息或者connLost标志
			if c.debugMode {
				fmt.Println("main routine client read req")
			}

			c.reader.GetNextMessageToChannelOrSetReq()

		case <-c.explicitCloseSetChannel:
			c.explicitClose = true

			if c.sender.getBufferSize() == 0 || c.connLost { // 说明已经可以退出了
				c.epochTicker.Stop()
				c.mainRoutineReadyExitChannel <- 0
			}

		case <-c.mainRoutineExitChannel:
			c.epochTicker.Stop()
			//fmt.Println("main routine exit")
			return
		}
	}
}

func (c *client) ConnID() int {
	return c.connId
}

func (c *client) Read() ([]byte, error) {

	c.clientReadRequest <- 0
	dataMessage := <-c.clientReadResponse
	//fmt.Println(dataMessage)
	if dataMessage.SeqNum < 0 { // 代表connLost且没有别的消息了
		return nil, errors.New("conn lost or explicit close: read")
	} else {
		/*fmt.Println("msg return:", dataMessage.String())
		fmt.Println("nextNumber:", c.clientReadSeqNumber)*/
		return dataMessage.Payload, nil
	}

}

func (c *client) Write(payload []byte) error {
	if c.connLost {
		return errors.New("conn lost or explicit close: write")
	}
	if c.debugMode {
		fmt.Println("message send:", string(payload))
	}
	// 由于此处的payload只是数据信息，需要封装包头
	c.clientWriteMessageChannel <- NewData(c.connId, 0, len(payload), payload)
	return nil

}

// 向routine发送信息，在对应的消息处理完之后 routine退出
func (c *client) Close() error {
	if c.debugMode {
		fmt.Println("client close")
	}

	// 标记退出
	c.explicitCloseSetChannel <- 0

	// 等待所有消息都send和ack, mainRoutine退出或者由于connLost或者explicitClose直接退出
	<-c.mainRoutineReadyExitChannel
	//fmt.Println("client main routine ready exit, write exit msg to read routine")

	// 首先关闭conn close 防止read routine在udpconn.read()处阻塞
	c.udpConn.Close()

	c.readRoutineExitChannel <- 0
	//fmt.Println("client read routine exit")

	c.mainRoutineExitChannel <- 0
	//fmt.Println("client main routine exit")

	return nil
}
