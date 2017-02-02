package lsp

import "container/list"

type Reader struct {
	readBuffer *list.List // 由receiver操控的有序data数据，供read(）读取
	readReq bool
	readResponseChannel chan *Message
}

func NewReader (responseChannel chan *Message) *Reader{
	return &Reader{list.New(), false, responseChannel}
}

func (reader *Reader) OfferMsgWithReqCheck(message *Message) {
	if reader.getReq() {
		reader.readResponseChannel <- message
		reader.resetReq()
	} else {
		reader.readBuffer.PushBack(message)
	}
}

func (reader *Reader) GetNextMessageToChannelOrSetReq()  {
	if reader.readBuffer.Len() > 0 {
		reader.readResponseChannel <- reader.poll()
	} else {
		reader.setReq()
	}
}

func (reader *Reader) poll() *Message{
	return reader.readBuffer.Remove(reader.readBuffer.Front()).(*Message)
}

func (reader *Reader) getReq() bool {
	return reader.readReq
}

func (reader *Reader) setReq() {
	reader.readReq = true
}

func (reader *Reader) resetReq() {
	reader.readReq = false
}
