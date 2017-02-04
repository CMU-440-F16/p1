package main

import (
	"fmt"
	"os"
	"p1/src/github.com/cmu440/bitcoin"
	"p1/src/github.com/cmu440/lsp"
	"strconv"
)

func main() {
	const numArgs = 4
	if len(os.Args) != numArgs {
		fmt.Printf("Usage: ./%s <hostport> <message> <maxNonce>", os.Args[0])
		return
	}
	hostport := os.Args[1]
	message := os.Args[2]
	maxNonce, err := strconv.ParseUint(os.Args[3], 10, 64)
	if err != nil {
		fmt.Printf("%s is not a number.\n", os.Args[3])
		return
	}

	client, err := lsp.NewClient(hostport, lsp.NewParams())
	if err != nil {
		fmt.Println("Failed to connect to server:", err)
		return
	}

	defer client.Close()

	requestMessage := bitcoin.MarshalMessage(bitcoin.NewRequest(message, 0, maxNonce))

	client.Write(requestMessage)

	result, err := client.Read()

	if err != nil { // nil的情况只有可能是连接丢失
		printDisconnected()
	} else {
		printRes := bitcoin.UnmarshalMessage(result)
		printResult(printRes.Hash, printRes.Nonce)
	}

}

// printResult prints the final result to stdout.
func printResult(hash, nonce uint64) {
	fmt.Println("Result", hash, nonce)
}

// printDisconnected prints a disconnected message to stdout.
func printDisconnected() {
	fmt.Println("Disconnected")
}
