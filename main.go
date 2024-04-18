package main

import (
	"fmt"
	"net"
	"strings"

	"github.com/Paintersrp/go-redis/internal/aof"
	"github.com/Paintersrp/go-redis/internal/handlers"
	"github.com/Paintersrp/go-redis/internal/resp"
	"github.com/Paintersrp/go-redis/internal/value"
	"github.com/Paintersrp/go-redis/internal/writer"
)

func main() {
	fmt.Println("Listening on port :6379")

	// Create a new server
	l, err := net.Listen("tcp", ":6379")
	if err != nil {
		fmt.Println(err)
		return
	}

	// Start AOF DB
	db, err := aof.NewAof("database.aof")
	if err != nil {
		fmt.Println(err)
		return
	}
	defer db.Close()

	// Read AOF DB into Memory
	db.Read(func(value value.Value) {
		command := strings.ToUpper(value.Array[0].Bulk)
		args := value.Array[1:]
		handler, ok := handlers.Handlers[command]

		if !ok {
			fmt.Println("Invalid command: ", command)
			return
		}

		handler(args)
	})

	// Listen for connections
	conn, err := l.Accept()
	if err != nil {
		fmt.Println(err)
		return
	}

	defer conn.Close()

	for {
		r := resp.NewResp(conn)
		v, err := r.Read()
		if err != nil {
			fmt.Println(err)
			return
		}

		if v.Typ != "array" {
			fmt.Println("Invalid request, expected array")
			continue
		}

		if len(v.Array) == 0 {
			fmt.Println("Invalid request, expected array length > 0")
			continue
		}

		command := strings.ToUpper(v.Array[0].Bulk)
		args := v.Array[1:]

		w := writer.NewWriter(conn)

		h, ok := handlers.Handlers[command]

		fmt.Println(command)

		if !ok {
			fmt.Println("Invalid command: ", command)
			w.Write(value.Value{Typ: "string", Str: ""})
			continue
		}

		if command == "SET" || command == "HSET" {
			db.Write(v)
		}

		result := h(args)
		w.Write(result)
	}
}
