// Contains the implementation of a LSP client.

package lsp

import (
	"p1/src/github.com/cmu440/lspnet"

	"encoding/json"

	"container/list"
	"fmt"
	"strconv"
	"time"
)

type client struct {
	debugMode           bool
	connId              int
	udpConn             *lspnet.UDPConn
	sendSeqNumber       int  // 和ack read, msg write有关 初始为0,(代表conn)，为每一个client发送的包进行编号
	readSeqNumber       int  // 和msg read, ack write(epoch)有关 初始为0, 代表下一个接受的消息的序号(小于该序号的dataMessage的丢弃)
	clientReadSeqNumber int  // 从0开始，随着每次client读取++，代表client想读取的消息seqNumber
	readReq             bool // 有请求设置为true, 等读取到了和clientReadSeqNumber相等的dataMsg并且为true时返回对应Message

	epochLimit  int
	epochTicker *time.Ticker
	windowSize  int
	windowStart int // client发送包的起始编号（没有ack）
	windowEnd   int // client发送包的终止编号

	sendBuffer    *list.List       // 缓存要发送，或者是已经发送但是没有ack的数据(ack之后的消息从buffer中移除)
	receiveBuffer map[int]*Message // 缓存接收到，但是没有被上层read的数据(被上层读取后从buffer中移除)

	clientDataMessageChannel  chan *Message
	clientAckMessageChannel   chan *Message
	clientWriteMessageChannel chan *Message // 如果同时write和read都修改sequence Number的话会出现race condition
	clientReadRequest         chan int      // client的read请求
	clientReadResponse        chan *Message // read请求的返回
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

	client := client{false, 0, udpConn, 0, 0, 0, false, params.EpochLimit, time.NewTicker(time.Millisecond * time.Duration(params.EpochMillis)), params.WindowSize,
		0, params.WindowSize - 1, list.New(), make(map[int]*Message), make(chan *Message), make(chan *Message), make(chan *Message), make(chan int), make(chan *Message)}

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
	if err != nil {
		// TODO 超时处理
	}

	client.connId, _ = strconv.Atoi(string(connID))

	return &client, nil
}

func (c *client) readRoutine() {
	for {
		select {
		default:
			buffer := make([]byte, MAX_MESSAGE_SIZE)

			len, err := c.udpConn.Read(buffer)
			if err != nil {

			}

			buffer = buffer[:len]
			var message Message
			json.Unmarshal(buffer, &message)
			if c.debugMode {
				fmt.Println("client received message:", message)
			}
			// 判断是ACK还是普通的Message(client 不会收到connMessage)
			switch message.Type {
			case MsgData:
				if c.debugMode {
					fmt.Println("plain message")
					fmt.Println("plain message size", message.Size)
				}
				c.clientWriteMessageChannel <- NewAck(c.connId, message.SeqNum)
				c.clientDataMessageChannel <- &message

			case MsgAck:
				if c.debugMode {
					fmt.Println("ack message received")
				}
				c.clientAckMessageChannel <- &message
			}

		}
	}
}

func (c *client) mainRoutine() {
	for {
		select {
		case <-c.epochTicker.C:
		// epoch事件 todo
		case ackMessage := <-c.clientAckMessageChannel:
			// 更新send buffer、如果刚好是windowStart的话更新sendSeqNumber、window start和window end

			for iter := c.sendBuffer.Front(); iter != nil; iter = iter.Next() {
				// 消息被ack了 从sendbuffer中移除 并适时地更新窗口信息，发送在窗口内的未发送数据
				var ackSeq int
				if ackSeq = iter.Value.(*Message).SeqNum; ackSeq == ackMessage.SeqNum {

					// 如果ack的是windowStart的消息, 则windowStart、End需要更新到后面的未ack消息, 同时发(oldEnd, newEnd]的buffer数据
					if iter == c.sendBuffer.Front() {
						// 例如conn的ack sendBuffer无缓存消息
						if iter.Next() == nil {
							c.windowStart = ackSeq + 1
							c.windowEnd = c.windowSize + c.windowStart - 1
						} else {
							oldEnd := c.windowEnd
							c.windowStart = iter.Next().Value.(*Message).SeqNum // 指向下一个没有ack的dataMsg
							c.windowEnd = c.windowSize + c.windowStart - 1

							// 此时需要将新窗口范围内的未发送数据发送,范围是(oldEnd, newEnd]
							for newIter := iter.Next(); newIter != nil; newIter = newIter.Next() {
								message := newIter.Value.(Message)
								if message.SeqNum > c.windowEnd {
									break
								}
								if message.SeqNum > oldEnd && message.SeqNum <= c.windowEnd {
									messageSend, _ := json.Marshal(message)
									c.udpConn.Write(messageSend)
								}

							}
						}
					}
					c.sendBuffer.Remove(iter)
					break
				}
			}
			// seqNumber为0的ack含有connId，需要缓存，为了和dataMsg一起处理，此处将payload设置为connId的byte数组
			if ackMessage.SeqNum == 0 {
				if c.debugMode {
					fmt.Println("conn ack received")
				}
				ackMessage.Payload = []byte(strconv.Itoa(ackMessage.ConnID))
				c.readSeqNumber++

				if c.readReq {
					c.readReq = false
					c.clientReadSeqNumber++
					if c.debugMode {
						fmt.Println("client read() seq:",c.clientReadSeqNumber)
					}
					c.clientReadResponse <- ackMessage

				} else {
					c.receiveBuffer[0] = ackMessage
				}


			}

		case dataMessage := <-c.clientDataMessageChannel:
			// 更新readSeqNumber、receive buffer等信息
			// 如果有readReq,则返回对应的信息 并更新readReq和clientReadNumber
			if c.debugMode {
				fmt.Println("client read data message:", string(dataMessage.Payload))
				fmt.Println(dataMessage.SeqNum, c.readSeqNumber)
			}
			if dataMessage.SeqNum >= c.readSeqNumber { // 重复的信息(小于client期望接受的seqNumber)丢弃
				// 缓存消息
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
				// 如果和上层期望的一致并且受到了请求 则清除cache并向response channel写入dataMsg，更新client下次读取的seqNumber
				if c.debugMode {
					fmt.Println(dataMessage.SeqNum, c.clientReadSeqNumber, c.readReq)
				}
				if dataMessage.SeqNum == c.clientReadSeqNumber && c.readReq {

					c.readReq = false
					delete(c.receiveBuffer, dataMessage.SeqNum)
					c.clientReadSeqNumber++
					if c.debugMode {
						fmt.Println("client read() seq:",c.clientReadSeqNumber)
					}
					c.clientReadResponse <- dataMessage
					if c.debugMode {
						fmt.Println("read get its payload")
					}
				}
			}


		case writeMessage := <-c.clientWriteMessageChannel:
			// ack消息直接写
			if writeMessage.Type == MsgAck {
				if c.debugMode {
					fmt.Println("client write ack:", writeMessage.SeqNum)
				}
				messageSend, _ := json.Marshal(writeMessage)
				c.udpConn.Write(messageSend)
			} else { // dataMessage或者Connect需要暂存

				c.sendBuffer.PushBack(writeMessage)

				// 如果在window的范围内则直接写
				if writeMessage.SeqNum >= c.windowStart && writeMessage.SeqNum <= c.windowEnd {
					messageSend, _ := json.Marshal(writeMessage)
					c.udpConn.Write(messageSend)
				}
				// 更新下一个发送的编号
				c.sendSeqNumber++
			}

		case <-c.clientReadRequest: // 标记收到了上方的read请求
			msg, ok := c.receiveBuffer[c.clientReadSeqNumber]
			if ok { // 已经有了则直接移除缓存并发送到response channel，更新下一个读取的data seqNumber
				delete(c.receiveBuffer, c.clientReadSeqNumber)
				c.clientReadSeqNumber++
				if c.debugMode {
					fmt.Println("client read() seq:",c.clientReadSeqNumber)
				}
				c.clientReadResponse <- msg
			} else { // 作标记，当对应的clientReadSeqNumber来的时候再更新
				c.readReq = true
			}

		}
	}
}

func (c *client) ConnID() int {
	return c.connId
}

func (c *client) Read() ([]byte, error) {
	c.clientReadRequest <- 0

	dataMessage := <-c.clientReadResponse

	return dataMessage.Payload, nil
}

func (c *client) Write(payload []byte) error {

	if c.debugMode {
		fmt.Println("message send:", string(payload))
	}
	// 由于此处的payload只是数据信息，需要封装包头
	c.clientWriteMessageChannel <- NewData(c.connId, c.sendSeqNumber, len(payload), payload)

	return nil
}

/*// 由于Write方法发送的是data message的payload
// 此处为了方便起见，将connect和Write分开，此处的connectMessage为Message类型marshal之后的结果
func (c *client) Connect(connMessage *Message) {
	c.clientWriteMessageChannel <- connMessage
}*/

func (c *client) Close() error {
	if c.debugMode {
		fmt.Println("client close")
	}
	return c.udpConn.Close()
}
