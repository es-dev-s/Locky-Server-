package main

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"strings"

	"github.com/gorilla/websocket"
)

var upgrader = websocket.Upgrader{
	ReadBufferSize:  1 << 16,
	WriteBufferSize: 1 << 16,
	// LAN tool: peers connect from arbitrary device origins on the local network.
	CheckOrigin: func(r *http.Request) bool { return true },
}

// clientAddr returns the best-effort remote IP (honours X-Forwarded-For from our proxy).
func clientAddr(r *http.Request) string {
	if fwd := r.Header.Get("X-Forwarded-For"); fwd != "" {
		parts := strings.Split(fwd, ",")
		return strings.TrimSpace(parts[0])
	}
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return r.RemoteAddr
	}
	return host
}

// hostLANPrefix returns the first private IPv4 /24 on this machine, e.g. "192.168.1.0/24".
func hostLANPrefix() string {
	ifaces, err := net.Interfaces()
	if err != nil {
		return ""
	}
	for _, iface := range ifaces {
		if iface.Flags&net.FlagUp == 0 || iface.Flags&net.FlagLoopback != 0 {
			continue
		}
		addrs, err := iface.Addrs()
		if err != nil {
			continue
		}
		for _, addr := range addrs {
			var ip net.IP
			switch v := addr.(type) {
			case *net.IPNet:
				ip = v.IP
			case *net.IPAddr:
				ip = v.IP
			}
			if ip == nil {
				continue
			}
			v4 := ip.To4()
			if v4 == nil || v4.IsLoopback() || v4.IsLinkLocalUnicast() {
				continue
			}
			if !v4.IsPrivate() {
				continue
			}
			return fmt.Sprintf("%d.%d.%d.0/24", v4[0], v4[1], v4[2])
		}
	}
	return ""
}

// roomKey groups nearby devices so a PC on localhost and a phone on Wi‑Fi
// see each other when talking to the same Locky host.
//
// Without this, the proxy's X-Forwarded-For puts localhost in "127.0.0.1"
// and the phone in "192.168.x.y" — separate rooms, empty peer lists.
func roomKey(r *http.Request) string {
	raw := clientAddr(r)
	ip := net.ParseIP(raw)
	if ip == nil {
		if lan := hostLANPrefix(); lan != "" {
			return lan
		}
		return "lan"
	}
	if ip.IsLoopback() {
		if lan := hostLANPrefix(); lan != "" {
			return lan
		}
		return "lan"
	}
	if v4 := ip.To4(); v4 != nil {
		return fmt.Sprintf("%d.%d.%d.0/24", v4[0], v4[1], v4[2])
	}
	if lan := hostLANPrefix(); lan != "" {
		return lan
	}
	return "lan"
}

func serveWS(hub *Hub, w http.ResponseWriter, r *http.Request) {
	name := strings.TrimSpace(r.URL.Query().Get("name"))
	if name == "" {
		name = "anonymous"
	}
	if len(name) > 48 {
		name = name[:48]
	}
	avatar := strings.TrimSpace(r.URL.Query().Get("avatar"))
	if len(avatar) > 96 {
		avatar = avatar[:96]
	}
	if avatar == "" {
		avatar = name
	}

	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Printf("upgrade failed: %v", err)
		return
	}

	peer := newPeer(hub, conn, name, avatar, roomKey(r))
	hub.register(peer)
	go peer.writePump()
	go peer.readPump()
}

func main() {
	hub := newHub()

	mux := http.NewServeMux()
	mux.HandleFunc("/ws", func(w http.ResponseWriter, r *http.Request) {
		serveWS(hub, w, r)
	})
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"ok":true}`))
	})
	mux.HandleFunc("/debug/rooms", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Access-Control-Allow-Origin", "*")
		_, _ = w.Write(hub.debugRoomsJSON())
	})

	addr := ":" + envOr("PORT", "8090")
	log.Printf("locky backend listening on %s (lan room hint: %s)", addr, orDash(hostLANPrefix()))
	if err := http.ListenAndServe(addr, mux); err != nil {
		log.Fatal(err)
	}
}

func orDash(s string) string {
	if s == "" {
		return "none"
	}
	return s
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
