// Copyright 2024 Lei Ni (nilei81@gmail.com)
//
// This library follows a dual licensing model -
//
// - it is licensed under the 2-clause BSD license if you have written evidence showing that you are a licensee of github.com/lni/pothos
// - otherwise, it is licensed under the GPL-2 license
//
// See the LICENSE file for details

package dht

import (
	"errors"
	"hash/crc32"
	"net"
	"os"
	"time"
)

const (
	maxPacketSize int = 4096
)

type conn struct {
	ReceivedCh chan message
	sendBuf    []byte
	recvBuf    []byte
	c          *net.UDPConn
}

func newConn(cfg Config) (*conn, error) {
	listener, err := net.ListenPacket(cfg.Proto, cfg.Address.String())
	if err != nil {
		return nil, err
	}

	return &conn{
		ReceivedCh: make(chan message, 128),
		sendBuf:    make([]byte, maxPacketSize),
		recvBuf:    make([]byte, maxPacketSize),
		c:          listener.(*net.UDPConn),
	}, nil
}

func (c *conn) Close() error {
	return c.c.Close()
}

func (c *conn) SendMessage(buf []byte, addr net.UDPAddr) error {
	if _, err := c.c.WriteTo(buf, &addr); err != nil {
		return err
	}

	return nil
}

func (c *conn) ReceiveMessageLoop(stopc chan struct{}) error {
	for {
		select {
		case <-stopc:
			return nil
		default:
		}

		timeout := time.Now().Add(10 * time.Millisecond)
		if err := c.c.SetReadDeadline(timeout); err != nil {
			return err
		}
		// FIXME: do we need to returned addr or just rely on the addr info in the
		// payload
		n, _, err := c.c.ReadFromUDP(c.recvBuf)
		if errors.Is(err, os.ErrDeadlineExceeded) {
			// timeout
			continue
		}
		if err != nil {
			continue
		}
		buf, ok := verifyReceivedMessage(c.recvBuf[:n])
		if !ok {
			continue
		}
		var msg message
		if err := msg.Unmarshal(buf); err != nil {
			// not a valid message
			continue
		}
		select {
		case <-stopc:
			return nil
		case c.ReceivedCh <- msg:
		}
	}
}

func getMessageBuf(buf []byte, msg message) ([]byte, error) {
	msgSize := msg.MarshalSize()
	// need extra 2 bytes tag, 2 bytes sz, 4 bytes crc
	bufSize := msgSize + 8
	if bufSize > len(buf) {
		buf = make([]byte, bufSize)
	}

	// tag
	buf[0] = magicNumber[0]
	buf[1] = magicNumber[1]
	// message payload size
	codec.PutUint16(buf[2:], uint16(msgSize))
	if _, err := msg.Marshal(buf[4:]); err != nil {
		return nil, err
	}
	// crc
	v := crc32.ChecksumIEEE(buf[2 : 4+msgSize])
	codec.PutUint32(buf[4+msgSize:], v)

	return buf[:bufSize], nil
}

func verifyReceivedMessage(msg []byte) ([]byte, bool) {
	if len(msg) < 8 {
		return nil, false
	}
	if msg[0] != magicNumber[0] || msg[1] != magicNumber[1] {
		return nil, false
	}
	sz := int(codec.Uint16(msg[2:]))
	if len(msg) != sz+8 {
		return nil, false
	}
	v := crc32.ChecksumIEEE(msg[2 : 4+sz])
	crcMsg := codec.Uint32(msg[4+sz:])
	if v != crcMsg {
		return nil, false
	}

	return msg[4 : 4+sz], true
}
