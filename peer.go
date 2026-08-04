package main

import (
	"encoding/binary"
	"encoding/json"
	"log"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

const (
	writeWait  = 15 * time.Second
	pongWait   = 70 * time.Second
	pingPeriod = 25 * time.Second
	// Chunks are 256 KiB from clients; leave generous headroom.
	maxMessageSize = 1<<20 + 1<<16
	sendBufferSize = 256
	// If a receiver cannot drain its queue for this long it is considered
	// dead/stuck and gets disconnected instead of stalling the whole room.
	slowPeerTimeout = 20 * time.Second
)

// Control message types that are relayed peer-to-peer verbatim (plus "from").
var routable = map[string]bool{
	"offer":    true,
	"answer":   true,
	"ack":      true,
	"eof":      true,
	"received": true,
	"cancel":   true,
	"progress": true,
}

type outbound struct {
	kind int
	data []byte
}

type Peer struct {
	ID     string
	Name   string
	Avatar string
	Room   string
	Ready  bool

	hub  *Hub
	conn *websocket.Conn
	send chan outbound

	closeOnce sync.Once
	done      chan struct{}
}

func newPeer(hub *Hub, conn *websocket.Conn, name, avatar, room string) *Peer {
	return &Peer{
		ID:     newID(),
		Name:   name,
		Avatar: avatar,
		Room:   room,
		Ready:  true,
		hub:    hub,
		conn:   conn,
		send:   make(chan outbound, sendBufferSize),
		done:   make(chan struct{}),
	}
}

func (p *Peer) info() peerInfo {
	return peerInfo{ID: p.ID, Name: p.Name, Avatar: p.Avatar, Ready: p.Ready}
}

func (p *Peer) close() {
	p.closeOnce.Do(func() {
		close(p.done)
		_ = p.conn.Close()
	})
}

func (p *Peer) sendJSON(msg map[string]any) {
	data, err := json.Marshal(msg)
	if err != nil {
		return
	}
	p.enqueue(outbound{websocket.TextMessage, data})
}

// enqueue blocks (bounded) when the receiver is slower than the sender,
// which propagates backpressure to the sending client through TCP.
func (p *Peer) enqueue(m outbound) bool {
	select {
	case p.send <- m:
		return true
	case <-p.done:
		return false
	case <-time.After(slowPeerTimeout):
		log.Printf("peer %s too slow, disconnecting", p.ID)
		p.close()
		return false
	}
}

func (p *Peer) readPump() {
	defer func() {
		p.hub.unregister(p)
		p.close()
	}()

	p.conn.SetReadLimit(maxMessageSize)
	_ = p.conn.SetReadDeadline(time.Now().Add(pongWait))
	p.conn.SetPongHandler(func(string) error {
		return p.conn.SetReadDeadline(time.Now().Add(pongWait))
	})

	for {
		kind, data, err := p.conn.ReadMessage()
		if err != nil {
			return
		}
		switch kind {
		case websocket.TextMessage:
			p.handleControl(data)
		case websocket.BinaryMessage:
			p.handleBinary(data)
		}
	}
}

func (p *Peer) writePump() {
	ticker := time.NewTicker(pingPeriod)
	defer func() {
		ticker.Stop()
		p.close()
	}()

	for {
		select {
		case msg, ok := <-p.send:
			if !ok {
				return
			}
			_ = p.conn.SetWriteDeadline(time.Now().Add(writeWait))
			if err := p.conn.WriteMessage(msg.kind, msg.data); err != nil {
				return
			}
		case <-ticker.C:
			_ = p.conn.SetWriteDeadline(time.Now().Add(writeWait))
			if err := p.conn.WriteMessage(websocket.PingMessage, nil); err != nil {
				return
			}
		case <-p.done:
			return
		}
	}
}

func (p *Peer) handleControl(data []byte) {
	var msg map[string]any
	if err := json.Unmarshal(data, &msg); err != nil {
		return
	}
	msgType, _ := msg["type"].(string)

	switch msgType {
	case "rename":
		name, _ := msg["name"].(string)
		name = strings.TrimSpace(name)
		if name == "" || len(name) > 48 {
			return
		}
		p.Name = name
		if avatar, ok := msg["avatar"].(string); ok {
			avatar = strings.TrimSpace(avatar)
			if len(avatar) > 96 {
				avatar = avatar[:96]
			}
			p.Avatar = avatar
		}
		p.hub.broadcast(p, map[string]any{
			"type": "peer-updated",
			"peer": p.info(),
		})
		return
	case "ready":
		ready, ok := msg["ready"].(bool)
		if !ok {
			return
		}
		p.Ready = ready
		p.hub.broadcast(p, map[string]any{
			"type": "peer-updated",
			"peer": p.info(),
		})
		return
	}

	if !routable[msgType] {
		return
	}
	to, _ := msg["to"].(string)
	if to == "" || to == p.ID {
		return
	}
	target := p.hub.get(p, to)
	if target == nil {
		p.sendJSON(map[string]any{
			"type":    "route-error",
			"tid":     msg["tid"],
			"message": "peer is no longer available",
		})
		return
	}
	delete(msg, "to")
	msg["from"] = p.ID
	target.sendJSON(msg)
}

// Binary frame layout: [4-byte BE header length][JSON header][chunk payload].
// Header carries {to, tid, item, seq, last}; the server swaps `to` for `from`.
func (p *Peer) handleBinary(data []byte) {
	if len(data) < 4 {
		return
	}
	headerLen := binary.BigEndian.Uint32(data[:4])
	if int(headerLen)+4 > len(data) || headerLen > 4096 {
		return
	}
	var header map[string]any
	if err := json.Unmarshal(data[4:4+headerLen], &header); err != nil {
		return
	}
	to, _ := header["to"].(string)
	if to == "" || to == p.ID {
		return
	}
	target := p.hub.get(p, to)
	if target == nil {
		p.sendJSON(map[string]any{
			"type":    "route-error",
			"tid":     header["tid"],
			"message": "peer is no longer available",
		})
		return
	}

	delete(header, "to")
	header["from"] = p.ID
	newHeader, err := json.Marshal(header)
	if err != nil {
		return
	}

	payload := data[4+headerLen:]
	frame := make([]byte, 4+len(newHeader)+len(payload))
	binary.BigEndian.PutUint32(frame[:4], uint32(len(newHeader)))
	copy(frame[4:], newHeader)
	copy(frame[4+len(newHeader):], payload)
	target.enqueue(outbound{websocket.BinaryMessage, frame})
}
