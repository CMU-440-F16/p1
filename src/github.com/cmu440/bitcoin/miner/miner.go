package main

import (
	"fmt"
	"os"

	"p1/src/github.com/cmu440/bitcoin"
	"p1/src/github.com/cmu440/lsp"
)

// Attempt to connect miner as a client to the server.
func joinWithServer(hostport string) (lsp.Client, error) {
	miner, err := lsp.NewClient(hostport, lsp.NewParams())
	if err != nil {
		fmt.Println("Failed to connect to server:", err)
		return nil, err
	}
	// miner的注册信息
	miner.Write(bitcoin.MarshalMessage(bitcoin.NewJoin()))

	return miner, nil
}

func findNonce(message string, lower, upper uint64) *bitcoin.Message {
	minHash := uint64(1<<64 - 1)
	minNonce := upper
	for nonce := lower; nonce <= upper; nonce++ {
		if hash := bitcoin.Hash(message, nonce); hash < minHash {
			minHash = hash
			minNonce = nonce
		}
	}
	res := bitcoin.NewResult(minHash, minNonce)
	res.Lower = lower // 为了上层处理的方便，此处标识Result的lower字段
	return res

}

// miner的主进程 每次读取一个request计算并返回，如果err则代表了conn Lost
// server端保证了不会给工作中的miner写入其他的请求，保证了miner失去了连接之后不会处理多余的pending request
func runMiner(client lsp.Client) {
	for {
		msg, err := client.Read()
		if err != nil { // 说明conn Lost
			return
		} else {
			requestMsg := bitcoin.UnmarshalMessage(msg)

			resultMsg := findNonce(requestMsg.Data, requestMsg.Lower, requestMsg.Upper)

			client.Write(bitcoin.MarshalMessage(resultMsg))
		}
	}
}

func main() {

	const numArgs = 2
	if len(os.Args) != numArgs {
		fmt.Printf("Usage: ./%s <hostport>", os.Args[0])
		return
	}

	hostport := os.Args[1]
	minerClient, err := joinWithServer(hostport)

	if err != nil {
		fmt.Println("Failed to join with server:", err)
		return
	}

	defer minerClient.Close()

	runMiner(minerClient)

}
