package lsp

type Receiver struct {
	receiveSeqNumber int // 和msg read, ack write(epoch)有关 client的Reader初始为1 (seq为0的ack需要读取，下同);server的proxy Reader初始为1，代表下一个接受的消息的序号(小于该序号的dataMessage的丢弃)
	receiveBuffer    map[int]*Message
}

func NewReceiver(receiveSeq int) *Receiver {
	return &Receiver{receiveSeq, make(map[int]*Message)}
}

// 缓存收到的消息，如果可以的话，将连续的有序消息存到readBuffer中
func (receiver *Receiver) BufferRecvMsgAndUpDate(dataMessage *Message, reader *Reader) {
	if _, ok := receiver.receiveBuffer[dataMessage.SeqNum]; dataMessage.SeqNum >= receiver.receiveSeqNumber && !ok { // 重复的信息(小于client期望接受的seqNumber或者buffer中已经含有)丢弃
		//fmt.Println("client expected msg:" , reader.readSeqNumber ,dataMessage.String())
		// 缓存读取的消息
		receiver.receiveBuffer[dataMessage.SeqNum] = dataMessage
		// 如果是下一个期望的dataMsg seqNumber 则将从连续的消息放入read()方法读取的缓存
		if dataMessage.SeqNum == receiver.receiveSeqNumber {
			reader.OfferMsgWithReqCheck(dataMessage)

			delete(receiver.receiveBuffer, receiver.receiveSeqNumber)
			receiver.receiveSeqNumber++
			// 将readSeqNumber更新到下一个没有收到的dataMsg seqNum
			for msg, ok := receiver.receiveBuffer[receiver.receiveSeqNumber]; ok; msg, ok = receiver.receiveBuffer[receiver.receiveSeqNumber] {
				reader.OfferMsgWithReqCheck(msg)
				delete(receiver.receiveBuffer, receiver.receiveSeqNumber)
				receiver.receiveSeqNumber++
			}
		}
	}
}

// connLost之后将剩余的消息存储到readBuffer
func (receiver *Receiver) movePendingMsgToReader(reader *Reader) {
	for size := len(receiver.receiveBuffer); size > 0; {
		msg, ok := receiver.receiveBuffer[receiver.receiveSeqNumber]
		if ok {
			reader.OfferMsgWithReqCheck(msg)
			delete(receiver.receiveBuffer, receiver.receiveSeqNumber)
			size--
		}
		receiver.receiveSeqNumber++
	}
}
