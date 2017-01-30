// Contains the implementation of a LSP client.

package lsp

import (
	"p1/src/github.com/cmu440/lspnet"

	"encoding/json"

	"container/list"
	"errors"
	"fmt"
	"strconv"
	"time"
)

type client struct {
	debugMode           bool
	connId              int
	udpConn             *lspnet.UDPConn
	sendSeqNumber       int  // 和ack read, msg write有关 初始为0,(包括conn)，为每一个client发送的包进行编号
	readSeqNumber       int  // 和msg read, ack write(epoch)有关 初始为0 (seq为0的ack需要读取，下同), 代表下一个接受的消息的序号(小于该序号的dataMessage的丢弃)
	clientReadSeqNumber int  // 从0开始，随着每次client读取++，代表client想读取的消息seqNumber(由于存在conn的ack 因此从0开始)
	readReq             bool // 有请求设置为true, 等读取到了和clientReadSeqNumber相等的dataMsg并且为true时返回对应Message

	epochLimit            int
	epochTicker           *time.Ticker
	windowSize            int
	windowStart           int  // client发送包的起始编号（没有ack,由于conn需要ack，所以从0开始）
	windowEnd             int  // client发送包的终止编号
	maxACKSeq             int  // 当窗口只有一个元素的时候，确定下一个发送的窗口
	mostRecentReceivedSeq int  // 最近收到的dataMsg的seqNumber
	epochReceived         bool // 标记epoch是否收到了任何数据
	noMsgEpochs           int
	connLost              bool
	explicitClose         bool

	sendBuffer    *list.List       // 缓存已经发送但是没有ack的数据或者是将要发送的数据(ack之后的消息从buffer中移除)
	receiveBuffer map[int]*Message // 缓存接收到，但是没有被上层read的数据(被上层读取后从buffer中移除)

	clientDataMessageChannel  chan *Message
	clientAckMessageChannel   chan *Message
	clientWriteMessageChannel chan *Message // 如果同时write和read都修改sequence Number的话会出现race condition
	clientReadRequest         chan int      // client的read请求
	clientReadResponse        chan *Message // read请求的返回
	clientWriteResult         chan int

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

	client := client{false, 0, udpConn, 0, 0, 0, false, params.EpochLimit, time.NewTicker(time.Millisecond * time.Duration(params.EpochMillis)), params.WindowSize,
		0, params.WindowSize - 1, 0, 0, false, 0, false, false, list.New(), make(map[int]*Message), make(chan *Message), make(chan *Message), make(chan *Message), make(chan int), make(chan *Message),
		make(chan int), make(chan int), make(chan int), make(chan int), make(chan int)}

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
				var message Message
				json.Unmarshal(buffer, &message)
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
					c.clientDataMessageChannel <- &message

				case MsgAck:
					//fmt.Println("client recv ack message:", message.String())
					if c.debugMode {
						fmt.Println("ack message received")
					}
					c.clientAckMessageChannel <- &message
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
			//fmt.Println("client epoch")
			//fmt.Println(c.noMsgEpochs)
			if c.noMsgEpochs >= c.epochLimit {
				c.connLost = true

				if c.explicitClose { // close阶段发生lost 直接return
					c.epochTicker.Stop() // 当lost且close之后才Stop定时器
					//fmt.Println("client with connLost after close, exit")
					c.mainRoutineReadyExitChannel <- 0
				} else if c.readReq { // 没有close, 但是connLost之前已经有read请求
					c.readReq = false
					c.getPendingMessageAfterLostOrExplicitClose()
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
					c.udpConn.Write(marShalMessage(NewConnect()))
				} else { // 发送mostRecentACK以及窗口内发送了但是没有ack的dataMsg

					c.udpConn.Write(marShalMessage(NewAck(c.connId, c.mostRecentReceivedSeq)))

					// 发送所有在sendBuffer [windosStart, windowEnd]中的数据
					for iter := c.sendBuffer.Front(); iter != nil; iter = iter.Next() {
						dataMsg := iter.Value.(*Message)
						if dataMsg.SeqNum > c.windowEnd {
							break
						}

						if dataMsg.SeqNum >= c.windowStart && dataMsg.SeqNum <= c.windowEnd {
							c.udpConn.Write(marShalMessage(dataMsg))
						}

					}
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
				c.maxACKSeq = max(c.maxACKSeq, ackMessage.SeqNum)
				for iter := c.sendBuffer.Front(); iter != nil; iter = iter.Next() {
					// 消息被ack了 从sendbuffer中移除 并适时地更新窗口信息，发送在窗口内的未发送数据
					var ackSeq int
					if ackSeq = iter.Value.(*Message).SeqNum; ackSeq == ackMessage.SeqNum {

						// 如果ack的是windowStart的消息, 则windowStart、End需要更新到后面的未ack消息, 同时发(oldEnd, newEnd]的buffer数据
						if iter == c.sendBuffer.Front() {
							// 例如conn的ack sendBuffer无缓存消息或者只有窗口的第一个没有ack，此时的start都是收到的maxACKSeq+1
							if iter.Next() == nil {
								// 所有消息都被ack了且调用了Close，则退出
								if c.explicitClose {
									//fmt.Println("client finish all ack exit")
									c.epochTicker.Stop()
									c.mainRoutineReadyExitChannel <- 0
								}
								c.windowStart = c.maxACKSeq + 1
								c.windowEnd = c.windowSize + c.windowStart - 1
							} else {
								oldEnd := c.windowEnd
								c.windowStart = iter.Next().Value.(*Message).SeqNum // 指向下一个没有ack的dataMsg
								c.windowEnd = c.windowSize + c.windowStart - 1

								// 此时需要将新窗口范围内的未发送数据发送,范围是(oldEnd, newEnd]
								for newIter := iter.Next(); newIter != nil; newIter = newIter.Next() {
									message := newIter.Value.(*Message)
									if message.SeqNum > c.windowEnd {
										break
									}
									if message.SeqNum > oldEnd && message.SeqNum <= c.windowEnd {
										c.udpConn.Write(marShalMessage(message))
									}

								}
							}
						}
						//fmt.Println("client remove send buffer message:", iter.Value.(*Message).String())
						c.sendBuffer.Remove(iter)
						//fmt.Println("buffer len:", c.sendBuffer.Len())
						//c.printBuffer();
						break
					}
				}
				// conn的ack消息(防止重复接受server端发送的conn ack作为上层read的结果)
				if ackMessage.SeqNum == 0 && c.connId == 0 {
					if c.debugMode {
						fmt.Println("conn ack received")
					}
					// seqNumber为0的ack含有connId，需要缓存，为了和dataMsg一起处理，此处将payload设置为connId的byte数组
					ackMessage.Payload = []byte(strconv.Itoa(ackMessage.ConnID))
					c.readSeqNumber++
					// client尝试了read()，则返回并更新
					if c.readReq {
						c.readReq = false
						c.clientReadSeqNumber++
						if c.debugMode {
							fmt.Println("client read() seq 234:", c.clientReadSeqNumber)
						}

						c.clientReadResponse <- ackMessage

					} else { // 没有则缓存
						c.receiveBuffer[0] = ackMessage
					}

				}
			}

		case dataMessage := <-c.clientDataMessageChannel:
			if !dataMsgSizeVA(dataMessage) {
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
				if c.debugMode {
					fmt.Println("client read data message:", string(dataMessage.Payload))
					fmt.Println(dataMessage.SeqNum, c.readSeqNumber)
				}
				c.mostRecentReceivedSeq = dataMessage.SeqNum
				if _, ok := c.receiveBuffer[dataMessage.SeqNum]; dataMessage.SeqNum >= c.readSeqNumber && !ok { // 重复的信息(小于client期望接受的seqNumber或者buffer中已经含有)丢弃
					//fmt.Println("client expected msg:" , c.readSeqNumber ,dataMessage.String())
					// 缓存读取的消息
					c.receiveBuffer[dataMessage.SeqNum] = dataMessage
					// 如果是下一个期望的dataMsg seqNumber 则更新readSeqNumber到第一个未接受的
					if dataMessage.SeqNum == c.readSeqNumber {
						c.readSeqNumber++
						// 将readSeqNumber更新到下一个没有收到的dataMsg seqNum
						for _, ok := c.receiveBuffer[c.readSeqNumber]; ok; _, ok = c.receiveBuffer[c.readSeqNumber] {
							c.readSeqNumber++
						}
						if c.debugMode {
							fmt.Println("after update, client read seq:", c.readSeqNumber)
						}
					}

					// 如果和上层期望的一致并且收到了read()请求 则清除cache并向response channel写入dataMsg，更新client下次读取的seqNumber
					if c.debugMode {
						fmt.Println(dataMessage.SeqNum, c.clientReadSeqNumber, c.readReq)
					}
					if dataMessage.SeqNum == c.clientReadSeqNumber && c.readReq {
						c.readReq = false
						delete(c.receiveBuffer, dataMessage.SeqNum)
						c.clientReadSeqNumber++
						if c.debugMode {
							fmt.Println("client read() seq:", c.clientReadSeqNumber)
						}
						c.clientReadResponse <- dataMessage
						if c.debugMode {
							fmt.Println("read get its payload")
						}
					}
				}
			}

		case writeMessage := <-c.clientWriteMessageChannel:
			if c.debugMode {
				fmt.Println("main routine write msg:", writeMessage)
			}
			if !c.connLost {
				// ack消息直接写
				if writeMessage.Type == MsgAck {
					if c.debugMode {
						fmt.Println("client write ack:", writeMessage.String())
					}
					c.udpConn.Write(marShalMessage(writeMessage))
				} else { // dataMessage或者Connect需要暂存
					writeMessage.SeqNum = c.sendSeqNumber
					c.sendSeqNumber++
					c.sendBuffer.PushBack(writeMessage)

					// 如果在window的范围内则直接写
					if writeMessage.SeqNum >= c.windowStart && writeMessage.SeqNum <= c.windowEnd {
						c.udpConn.Write(marShalMessage(writeMessage))
					}

					if writeMessage.Type == MsgData {
						c.clientWriteResult <- 1
					}
				}
			} else {
				if writeMessage.Type == MsgData {
					c.clientWriteResult <- 0
				}
			}

		case <-c.clientReadRequest: // 标记收到了上方的read请求, 可能是读取消息或者connLost标志
			if c.debugMode {
				fmt.Println("main routine client read req")
			}
			//fmt.Println("client read data msg")
			if !c.connLost {
				msg, ok := c.receiveBuffer[c.clientReadSeqNumber]
				if ok { // 已经有了则直接移除缓存并发送到response channel，更新下一个读取的data seqNumber
					//fmt.Println("client read data msg existd:", msg.String())
					delete(c.receiveBuffer, c.clientReadSeqNumber)
					c.clientReadSeqNumber++
					if c.debugMode {
						fmt.Println("client read() seq:", c.clientReadSeqNumber)
					}
					c.clientReadResponse <- msg
				} else { // 此时对应的消息还没有读取到，作标记，当对应的clientReadSeqNumber来的时候再更新
					//fmt.Println("client read data msg doesn't existd, tag to true")
					c.readReq = true
				}
			} else { // conn lost
				c.getPendingMessageAfterLostOrExplicitClose()
			}

		case <-c.explicitCloseSetChannel:
			c.explicitClose = true

			//fmt.Println("client mark close, unack msg size:", c.sendBuffer.Len())

			if c.sendBuffer.Len() == 0 || c.connLost{ // 说明已经可以退出了
				c.epochTicker.Stop()
				//fmt.Println("client with all msg acked before close, exit")

				c.mainRoutineReadyExitChannel <- 0
			}
		case <-c.mainRoutineExitChannel:
			c.epochTicker.Stop()
			//fmt.Println("main routine exit")
			return
		}
	}
}

// conn丢失但是有未读取的message的时候，按序返回message
func (c *client) getPendingMessageAfterLostOrExplicitClose() {
	if len(c.receiveBuffer) > 0 {
		for _, ok := c.receiveBuffer[c.clientReadSeqNumber]; !ok; _, ok = c.receiveBuffer[c.clientReadSeqNumber] {
			c.clientReadSeqNumber++
		}
		msg := c.receiveBuffer[c.clientReadSeqNumber]
		delete(c.receiveBuffer, c.clientReadSeqNumber)
		c.clientReadResponse <- msg
	} else { // connID为0代表没有多余的消息了
		c.clientReadResponse <- NewData(0, 0, 0, nil)
	}
}

func (c *client) ConnID() int {
	return c.connId
}

func (c *client) Read() ([]byte, error) {

	c.clientReadRequest <- 0
	dataMessage := <-c.clientReadResponse
	if dataMessage.ConnID == 0 { // 代表connLost或者explicit Close且没有别的消息了
		return nil, errors.New("conn lost or explicit close: read")
	} else {
		/*fmt.Println("msg return:", dataMessage.String())
		fmt.Println("nextNumber:", c.clientReadSeqNumber)*/
		return dataMessage.Payload, nil
	}

}

func (c *client) Write(payload []byte) error {

	if c.debugMode {
		fmt.Println("message send:", string(payload))
	}
	// 由于此处的payload只是数据信息，需要封装包头
	c.clientWriteMessageChannel <- NewData(c.connId, 0, len(payload), payload)

	res := <-c.clientWriteResult
	if res == 1 {
		return nil
	} else {
		return errors.New("conn lost or explicit close: write")
	}

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