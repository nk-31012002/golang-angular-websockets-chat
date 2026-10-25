package main

import (
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/google/uuid"
	"github.com/gorilla/websocket"
)

// Message defines our JSON payload format
type Message struct {
	Sender    string `json:"sender,omitempty"`
	Recipient string `json:"recipient,omitempty"`
	Content   string `json:"content,omitempty"`
}

// Client represents a connected websocket user
type Client struct {
	id     string
	socket *websocket.Conn
	send   chan []byte
}

// ClientManager coordinates all connected clients and message distribution
type ClientManager struct {
	clients    map[*Client]bool
	broadcast  chan []byte
	register   chan *Client
	unregister chan *Client
}

// Initialize the central manager instance
var manager = ClientManager{
	broadcast:  make(chan []byte),
	register:   make(chan *Client),
	unregister: make(chan *Client),
	clients:    make(map[*Client]bool),
}

// start runs a single coordinator loop to avoid race conditions
func (manager *ClientManager) start() {
	for {
		select {
		case conn := <-manager.register:
			// Register new connection in the pool
			manager.clients[conn] = true
			msg, _ := json.Marshal(&Message{Content: "/A new socket has connected."})
			manager.send(msg, conn)

		case conn := <-manager.unregister:
			// Deregister and clean up channels safely
			if _, ok := manager.clients[conn]; ok {
				close(conn.send)
				delete(manager.clients, conn)
				msg, _ := json.Marshal(&Message{Content: "/A socket has disconnected."})
				manager.send(msg, conn)
			}

		case message := <-manager.broadcast:
			// Distribute message to every active connection
			for conn := range manager.clients {
				select {
				case conn.send <- message:
				default:
					// If the buffer is clogged or unreachable, purge the client
					close(conn.send)
					delete(manager.clients, conn)
				}
			}
		}
	}
}

// send broadcasts payload to all clients except the 'ignore' client
func (manager *ClientManager) send(message []byte, ignore *Client) {
	for conn := range manager.clients {
		if conn != ignore {
			conn.send <- message
		}
	}
}

// read pulls incoming socket frames and puts them into the broadcast channel
func (c *Client) read() {
	defer func() {
		manager.unregister <- c
		c.socket.Close()
	}()

	for {
		_, message, err := c.socket.ReadMessage()
		if err != nil {
			break
		}
		// Wrap raw text payload into structured JSON Message
		jsonMessage, _ := json.Marshal(&Message{Sender: c.id, Content: string(message)})
		manager.broadcast <- jsonMessage
	}
}

// write pushes outgoing byte slices from client channel to network socket
func (c *Client) write() {
	defer func() {
		c.socket.Close()
	}()

	for message := range c.send {
		err := c.socket.WriteMessage(websocket.TextMessage, message)
		if err != nil {
			break
		}
	}
}

// wsPage handles incoming HTTP upgrades to WebSocket
func wsPage(res http.ResponseWriter, req *http.Request) {
	upgrader := websocket.Upgrader{
		// CheckOrigin allows connections from Angular running on localhost:4200
		CheckOrigin: func(r *http.Request) bool { return true },
	}

	conn, err := upgrader.Upgrade(res, req, nil)
	if err != nil {
		http.NotFound(res, req)
		return
	}

	client := &Client{
		id:     uuid.New().String(),
		socket: conn,
		send:   make(chan []byte, 256), // Buffered channel prevents deadlocks
	}

	manager.register <- client
	go client.read()
	go client.write()
}

func main() {
	fmt.Println("Starting Go WebSocket Server on :12345...")
	go manager.start()

	http.HandleFunc("/ws", wsPage)
	err := http.ListenAndServe(":12345", nil)
	if err != nil {
		fmt.Printf("Server failed: %v\n", err)
	}
}