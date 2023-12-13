package Server

import (
	"fmt"
	"io"
	"log"
	"net"
)

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

		// TODO: Neat to show (multithreading)
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

	fmt.Printf("Received: %s\n", buffer)

	// TODO: Convert buffer to string, create parser

	// TODO: Send data back to the client
	_, err = conn.Write([]byte("Message received.\n"))
	if err != nil {
		log.Println(err.Error())
		return
	}
}
