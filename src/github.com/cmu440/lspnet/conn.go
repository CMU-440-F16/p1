// DO NOT MODIFY THIS FILE!

package lspnet

import (
	"encoding/json"
	"errors"
	"log"
	"math/rand"
	"net"
	"sync/atomic"
)

var enableDebugLogs uint32

// This really shouldn't be here as it is a break of abstraction,
// but it is a minor hack to vary the payload length.
type TemporaryMessage struct {
	Type    int
	ConnID  int
	SeqNum  int
	Size    int
	Payload []byte
}

// EnableDebugLogs has log messages directed to standard output if enable is true.
func EnableDebugLogs(enable bool) {
	if enable {
		atomic.StoreUint32(&enableDebugLogs, 1)
	} else {
		atomic.StoreUint32(&enableDebugLogs, 0)
	}
}

func isLoggingEnabled() bool {
	return atomic.LoadUint32(&enableDebugLogs) == 1
}

// UDPConn is a wrapper around net.UDPConn. Method invocations are for the most part
// proxied directly to the corresponding methods in the net.UDPConn packge, but provide
// some additional book-keeping that is necessary for testing the students' code.
type UDPConn struct {
	nconn *net.UDPConn
}

// Read implements the Conn Read method.
func (c *UDPConn) Read(b []byte) (n int, err error) {
	var buffer [2000]byte
	for {
		n, err = c.nconn.Read(buffer[0:])
		if sometimes(readDropPercent(c)) {
			if isLoggingEnabled() {
				log.Printf("DROPPING read packet of length %d\n", n)
			}
		} else {
			copy(b, buffer[0:])
			break
		}
	}
	return n, err
}

// ReadFromUDP reads a UDP packet from c, copying the payload into b.
// It returns the number of bytes copied into b and the return address that
// was on the packet.
func (c *UDPConn) ReadFromUDP(b []byte) (n int, addr *UDPAddr, err error) {
	var naddr *net.UDPAddr
	var buffer [2000]byte
	for {
		n, naddr, err = c.nconn.ReadFromUDP(buffer[0:])
		if sometimes(readDropPercent(c)) {
			if isLoggingEnabled() {
				log.Printf("DROPPING read packet of length %d\n", n)
			}
		} else {
			copy(b, buffer[0:])
			if naddr != nil {
				addr = &UDPAddr{naddr: naddr}
			}
			break
		}
	}
	return n, addr, err
}

// Write implements the Conn Write method.
func (c *UDPConn) Write(b []byte) (int, error) {
	return c.write(b, nil)
}

// WriteToUDP writes a UDP packet to addr via c, copying the payload from b.
func (c *UDPConn) WriteToUDP(b []byte, addr *UDPAddr) (int, error) {
	if addr == nil {
		return 0, errors.New("addr must not be nil")
	}
	return c.write(b, addr)
}

func (c *UDPConn) write(b []byte, addr *UDPAddr) (int, error) {
	if sometimes(writeDropPercent(c)) {
		if isLoggingEnabled() {
			log.Printf("DROPPING written packet of length %d\n", len(b))
		}
		// Drop it, but make it look like it was successful.
		return len(b), nil
	}

	// This uses semantic packet data (i.e. assumes it's a "Message").
	// This is not optimal and breaks an abstraction, but is sufficient
	// for the task at hand.
	var msg TemporaryMessage
	err := json.Unmarshal(b, &msg)
	if err != nil {
		log.Printf("This should never be reached")
	}

	if msg.Type == 1 {
		shorten := sometimes(int(atomic.LoadUint32(&msgShorteningPercent)))
		lengthen := sometimes(int(atomic.LoadUint32(&msgLengtheningPercent)))

		if shorten {
			var payload int
			err = json.Unmarshal(msg.Payload, &payload)
			if err != nil {
				shorterPayload, _ := json.Marshal(payload / 1000)
				msg.Payload = shorterPayload
			} else {
				msg.Payload = msg.Payload[:len(msg.Payload)/2]
			}
		} else if lengthen {
			var payload int
			err = json.Unmarshal(msg.Payload, &payload)
			if err != nil {
				longerPayload, _ := json.Marshal(payload * 1000)
				msg.Payload = longerPayload
			} else {
				msg.Payload = append(msg.Payload, 2, 3, 4)
			}
		}

		if shorten || lengthen {
			b, _ = json.Marshal(msg)
		}
	}

	if addr == nil {
		n, err := c.nconn.Write(b)
		if err != nil {
			return 0, nil
		}
		return n, nil
	}
	return c.nconn.WriteToUDP(b, addr.toNet())
}

// Close closes the connection.
func (c *UDPConn) Close() error {
	mapMutex.Lock()
	if _, ok := connectionMap[*c]; ok {
		// Confirm that the connection exists (just in case).
		delete(connectionMap, *c)
	}
	mapMutex.Unlock()
	return c.nconn.Close()
}

func sometimes(percentage int) bool {
	return rand.Intn(100) < percentage
}
