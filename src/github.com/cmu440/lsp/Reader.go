package lsp

type Reader struct {
	receiveSeqNumber int // 和msg read, ack write(epoch)有关 client的Reader初始为1 (seq为0的ack需要读取，下同);server的proxy Reader初始为1，代表下一个接受的消息的序号(小于该序号的dataMessage的丢弃)
	readSeqNumber    int // 随着每次client读取++，代表client想读取的消息seqNumber(序号同上)
	receiveBuffer    map[int]*Message
}

func NewReader(receiveSeq, readSeq int) *Reader {
	return &Reader{receiveSeq, readSeq, make(map[int]*Message)}
}

/*func*/
