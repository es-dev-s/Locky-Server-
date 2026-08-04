package main

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"log"
	"sync"
)

// Hub keeps rooms of peers grouped by network identity.
// Nothing is persisted: state lives only while sockets are open.
type Hub struct {
	mu    sync.Mutex
	rooms map[string]map[string]*Peer
}

func newHub() *Hub {
	return &Hub{rooms: make(map[string]map[string]*Peer)}
}

func newID() string {
	b := make([]byte, 6)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

type peerInfo struct {
	ID     string `json:"id"`
	Name   string `json:"name"`
	Avatar string `json:"avatar,omitempty"`
	Ready  bool   `json:"ready"`
}

func (h *Hub) register(p *Peer) {
	h.mu.Lock()
	room := h.rooms[p.Room]
	if room == nil {
		room = make(map[string]*Peer)
		h.rooms[p.Room] = room
	}
	others := make([]peerInfo, 0, len(room))
	for _, other := range room {
		others = append(others, other.info())
	}
	room[p.ID] = p
	h.mu.Unlock()

	p.sendJSON(map[string]any{
		"type":  "hello",
		"self":  p.info(),
		"peers": others,
	})
	h.broadcast(p, map[string]any{
		"type": "peer-joined",
		"peer": p.info(),
	})
	log.Printf("peer %s (%s) joined room %s", p.ID, p.Name, p.Room)
}

func (h *Hub) unregister(p *Peer) {
	h.mu.Lock()
	room := h.rooms[p.Room]
	if room != nil {
		if room[p.ID] == p {
			delete(room, p.ID)
		}
		if len(room) == 0 {
			delete(h.rooms, p.Room)
		}
	}
	h.mu.Unlock()

	h.broadcast(p, map[string]any{"type": "peer-left", "peerId": p.ID})
	log.Printf("peer %s (%s) left room %s", p.ID, p.Name, p.Room)
}

// get returns a peer in the same room as `from`, or nil.
func (h *Hub) get(from *Peer, id string) *Peer {
	h.mu.Lock()
	defer h.mu.Unlock()
	room := h.rooms[from.Room]
	if room == nil {
		return nil
	}
	return room[id]
}

// broadcast sends a control message to everyone in p's room except p.
func (h *Hub) broadcast(p *Peer, msg map[string]any) {
	h.mu.Lock()
	room := h.rooms[p.Room]
	targets := make([]*Peer, 0, len(room))
	for _, other := range room {
		if other != p {
			targets = append(targets, other)
		}
	}
	h.mu.Unlock()

	for _, t := range targets {
		t.sendJSON(msg)
	}
}

func (h *Hub) debugRoomsJSON() []byte {
	h.mu.Lock()
	defer h.mu.Unlock()
	out := make(map[string][]peerInfo, len(h.rooms))
	for room, peers := range h.rooms {
		list := make([]peerInfo, 0, len(peers))
		for _, p := range peers {
			list = append(list, p.info())
		}
		out[room] = list
	}
	b, _ := json.Marshal(out)
	return b
}
