package lsp

import (
	"container/list"
	"p1/src/github.com/cmu440/lspnet"
)

type Sender struct {
	udpConn       *lspnet.UDPConn
	addr          *lspnet.UDPAddr // server的proxy需要由对应client的地址，client的addr为nil
	sendSeqNumber int             // 和ack read, msg write有关 初始为(client端的为0，包括conn; server端的proxy为1)，为每一个client发送的包进行编号
	windowSize    int
	windowStart   int // client发送包的起始编号（同sendSeqNumber）
	windowEnd     int
	sendBuffer    *list.List // 缓存已经发送但是没有ack的数据或者是将要发送的数据(ack之后的消息从buffer中移除)
	maxACKSeq     int        // 用于windowStart的包最后ack情况下的下一个窗口确定
}

func NewSender(conn *lspnet.UDPConn, addr *lspnet.UDPAddr, sendSeq, wSize, wStart int) *Sender {
	return &Sender{conn, addr, sendSeq, wSize, wStart, wStart + wSize - 1, list.New(), 0}
}

// client调用write发送消息
func (sender *Sender) SendAndBufferMsg(writeMessage *Message, buffer bool) {
	if writeMessage.Type == MsgAck {
		Send(sender.udpConn, writeMessage, sender.addr)
	} else { // dataMessage或者Connect需要暂存
		writeMessage.SeqNum = sender.sendSeqNumber
		sender.sendSeqNumber++
		if buffer {
			sender.sendBuffer.PushBack(writeMessage)
		}

		// 如果在window的范围内则直接写
		if writeMessage.SeqNum >= sender.windowStart && writeMessage.SeqNum <= sender.windowEnd {
			Send(sender.udpConn, writeMessage, sender.addr)
		}
	}
}

// 发送窗口内的dataMsg
// 如果iter为nil，则从头开始遍历(如epoch发送窗口内的unack dataMsg)
// iter不为nil的情况如ack消息更新了发送窗口
func (sender *Sender) SendWindowDataMsgInRange(iter *list.Element, start, end int) {
	if iter == nil {
		iter = sender.sendBuffer.Front()
	}
	for ; iter != nil; iter = iter.Next() {
		dataMsg := iter.Value.(*Message)
		if dataMsg.SeqNum > end {
			break
		}

		if dataMsg.SeqNum >= start && dataMsg.SeqNum <= end {
			Send(sender.udpConn, dataMsg, sender.addr)
		}

	}
}

// 根据ackSeq更新发送窗口
func (sender *Sender) UpdateSendBuffer(ackSeqNumber int) {
	sender.updateMaxAckSeq(ackSeqNumber)
	for iter := sender.sendBuffer.Front(); iter != nil; iter = iter.Next() {
		// 消息被ack了 从sendbuffer中移除 并适时地更新窗口信息，发送在窗口内的未发送数据

		if ackSeq := iter.Value.(*Message).SeqNum; ackSeq == ackSeqNumber {

			// 如果ack的是windowStart的消息, 则windowStart、End需要更新到后面的未ack消息, 同时发(oldEnd, newEnd]的buffer数据
			if iter == sender.sendBuffer.Front() {
				// 例如conn的ack sendBuffer无缓存消息或者只有窗口的第一个没有ack，此时的start都是收到的maxACKSeq+1
				if iter.Next() == nil {
					// 更新发送窗口为maxACKSeq+1为start
					sender.updateSendWindow(sender.maxACKSeq + 1)

				} else { // 否则window start为buffer中下一个元素的seq
					oldEnd := sender.windowEnd
					sender.updateSendWindow(iter.Next().Value.(*Message).SeqNum)

					// 此时需要将新窗口范围内的未发送数据发送,范围是(oldEnd, newEnd]
					sender.SendWindowDataMsgInRange(iter.Next(), oldEnd+1, sender.windowEnd)

				}
			}
			//fmt.Println("client remove send buffer message:", iter.Value.(*Message).String())
			sender.sendBuffer.Remove(iter)
			//fmt.Println("buffer len:", sender.sendBuffer.Len())
			//sender.printBuffer();
			break
		}
	}
}

func (sender *Sender) updateSendWindow(wStart int) {
	sender.windowStart = wStart
	sender.windowEnd = sender.windowStart + sender.windowSize - 1
}

func (sender *Sender) getBufferSize() int {
	return sender.sendBuffer.Len()
}

func (sender *Sender) updateMaxAckSeq(seq int) {
	sender.maxACKSeq = Max(sender.maxACKSeq, seq)
}

func (sender *Sender) getWindowStart() int {
	return sender.windowStart
}

func (sender *Sender) getWindowEnd() int {
	return sender.windowEnd
}
