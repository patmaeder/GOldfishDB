package Server

import (
	"io"
	"log"
	"net"
	"strings"
)

var callback func(buffer []byte) []byte

func Start() {
	log.Println("Starting TCP Server...")

	listener, err := net.Listen("tcp", "localhost:8080")
	if err != nil {
		log.Fatalln(err.Error())
	}

	log.Println("Server is listening on port 8080")
	defer func(listener net.Listener) {
		err := listener.Close()
		if err != nil {
			log.Fatalln(err.Error())
		}
	}(listener)

	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Println(err.Error())
		}

		go handleConnection(conn)
	}
}

func handleConnection(conn net.Conn) {
	defer func(conn net.Conn) {
		err := conn.Close()
		if err != nil {
			log.Println(err.Error())
		}
	}(conn)

	buffer, err := io.ReadAll(conn)
	if err != nil {
		log.Println(err.Error())
		return
	}

	log.Printf("Incoming request: %s %q", conn.RemoteAddr().String(), strings.TrimSpace(string(buffer)))

	res := callback(buffer)

	_, err = conn.Write(res)
	if err != nil {
		log.Println(err.Error())
		return
	}
}

func Handle(cb func(buffer []byte) []byte) {
	callback = cb
}
