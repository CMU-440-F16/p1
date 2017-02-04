package main

import (
	"fmt"
	"log"
	"os"
	"strconv"

	"container/list"
	"p1/src/github.com/cmu440/bitcoin"
	"p1/src/github.com/cmu440/lsp"
	"strings"
)

var SPLIT_THRESHOLD uint64 = 1000
var INIT_HASH uint64 = uint64(1<<64 - 1)

type splitRequest struct {
	splitCount  int
	subRequests map[uint64]*bitcoin.Message // 某个request切分之后的subRequest id为subRequest的lower
	nonce       uint64
	hash        uint64
}

func (splitReq *splitRequest) updateFinalResult(result *bitcoin.Message) {
	if result.Hash < splitReq.hash {
		splitReq.hash = result.Hash
		splitReq.nonce = result.Nonce
		splitReq.splitCount--
	}
}

func (splitReq *splitRequest) isFinish() bool {
	return splitReq.splitCount == 0
}

type server struct {
	lspServer           lsp.Server
	minerMap            map[int]bool
	availableMinersList *list.List
	splitRequests       map[int]*splitRequest // server所有的requestMap, id为client的connID
	assignMap           map[int]string        // 分配给miner的subRequest, id为miner id, value为 "clientID,subRequest_lower"
	subRequestList      *list.List            // 方便miner获得下一个subRequest 其元素为"clientID,subRequest_lower"
}

func (server *server) runServer() {
	for {
		connID, msgRead, err := server.lspServer.Read()
		if err != nil {
			if connID == 0 {
				// server close
				return
			} else {
				// 说明miner client的close或者是conn Lost
				if server.isMiner(connID) {
					clientID, lower := getClientIDandlower(server.assignMap[connID])

					// 该reg优先处理,插入首部
					server.subRequestList.InsertBefore(getSubReqIdentifier(clientID, lower), server.subRequestList.Front())
					server.dispatchSubRequest()
				} else {
					delete(server.splitRequests, connID) //从server中删除subRequests
				}
			}
		} else {
			message := bitcoin.UnmarshalMessage(msgRead)
			switch message.Type {
			case bitcoin.Join: // miner的join信息
				server.minerMap[connID] = true
				server.availableMinersList.PushBack(connID)
				server.dispatchSubRequest()
			case bitcoin.Request: // 先切分
				splitReq := server.splitRequest(connID, message)
				server.splitRequests[connID] = splitReq
				server.dispatchSubRequest()
			case bitcoin.Result:
				clientID, _ := getClientIDandlower(server.assignMap[connID])
				delete(server.assignMap, connID)            //表明该connID对应的miner不再与该subReq关联
				server.availableMinersList.PushBack(connID) // 表明miner空闲
				splitReq, ok := server.splitRequests[clientID]
				if !ok { // 如果client失去了连接 则对应的subrequests已经从server.requests删除，此处跳过后续操作
					break
				}
				splitReq.updateFinalResult(message)

				if splitReq.isFinish() {
					// 如果完成了所有的subReq，则将splitReq删除并向clientID对应的client回写结果

					server.lspServer.Write(clientID, bitcoin.MarshalMessage(bitcoin.NewResult(splitReq.hash, splitReq.nonce)))
					delete(server.splitRequests, clientID)
				}
				server.dispatchSubRequest()
			}
		}
	}
}

func (server *server) isMiner(connID int) bool {
	_, ok := server.minerMap[connID]

	return ok
}

func (server *server) dispatchSubRequest() {
	msize := server.availableMinersList.Len()
	reqsize := server.subRequestList.Len()
	for msize > 0 && reqsize > 0 {
		clientID, lower := getClientIDandlower(server.subRequestList.Remove(server.subRequestList.Front()).(string))

		reqMsg := server.splitRequests[clientID].subRequests[lower]
		miner := server.availableMinersList.Remove(server.availableMinersList.Front()).(int)

		server.lspServer.Write(miner, bitcoin.MarshalMessage(reqMsg))
		server.assignMap[miner] = getSubReqIdentifier(clientID, lower) // 标记miner的subReq
		msize--
		reqsize--
	}
}

func (server *server) splitRequest(connID int, request *bitcoin.Message) *splitRequest {
	splitRes := splitRequest{0, make(map[uint64]*bitcoin.Message), 0, INIT_HASH}

	var lower uint64
	var upper uint64
	for lower = 0; lower <= request.Upper; lower = upper + 1 {
		upper = lower + SPLIT_THRESHOLD
		if upper >= request.Upper {
			upper = request.Upper
			splitRes.splitCount++
			splitRes.subRequests[lower] = bitcoin.NewRequest(request.Data, lower, upper)
			server.subRequestList.PushBack(getSubReqIdentifier(connID, lower))
			break
		} else {
			splitRes.splitCount++
			splitRes.subRequests[lower] = bitcoin.NewRequest(request.Data, lower, upper)
			server.subRequestList.PushBack(getSubReqIdentifier(connID, lower))

		}
	}

	return &splitRes
}

func getSubReqIdentifier(clientID int, lower uint64) string {
	return strconv.Itoa(clientID) + "," + strconv.FormatUint(lower, 10)
}

func getClientIDandlower(s string) (int, uint64) {

	res := strings.Split(s, ",")

	u, _ := strconv.ParseUint(res[1], 10, 0)
	i, _ := strconv.Atoi(res[0])
	return i, u
}

func startServer(port int) (*server, error) {
	lspServer, err := lsp.NewServer(port, lsp.NewParams())
	if err != nil {
		return nil, err
	}
	server := server{lspServer, make(map[int]bool), list.New(), make(map[int]*splitRequest), make(map[int]string), list.New()}

	return &server, nil
}

var LOGF *log.Logger

func main() {
	// You may need a logger for debug purpose
	const (
		name = "log.txt"
		flag = os.O_RDWR | os.O_CREATE
		perm = os.FileMode(0666)
	)

	file, err := os.OpenFile(name, flag, perm)
	if err != nil {
		return
	}
	defer file.Close()

	LOGF = log.New(file, "", log.Lshortfile|log.Lmicroseconds)
	// Usage: LOGF.Println() or LOGF.Printf()

	const numArgs = 2
	if len(os.Args) != numArgs {
		fmt.Printf("Usage: ./%s <port>", os.Args[0])
		return
	}

	port, err := strconv.Atoi(os.Args[1])
	if err != nil {
		fmt.Println("Port must be a number:", err)
		return
	}

	srv, err := startServer(port)
	if err != nil {
		fmt.Println(err.Error())
		return
	}
	fmt.Println("Server listening on port", port)

	defer srv.lspServer.Close()

	srv.runServer()
}
