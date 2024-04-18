package handlers

import (
	"sync"

	"github.com/Paintersrp/go-redis/internal/value"
)

var Handlers = map[string]func([]value.Value) value.Value{
	"PING":    ping,
	"SET":     set,
	"GET":     get,
	"HSET":    hset,
	"HGET":    hget,
	"HGETALL": hgetall,
	"COMMAND": func(args []value.Value) value.Value { return value.Value{Typ: "string", Str: "OK"} },
}

var (
	SETs   = map[string]string{} // hash map
	SETsMu = sync.RWMutex{}      // hash map mutex

	HSETs   = map[string]map[string]string{} // nested hashmaps
	HSETsMu = sync.RWMutex{}                 // nested hashmap mutex
)

func ping(args []value.Value) value.Value {
	if len(args) == 0 {
		return value.Value{Typ: "string", Str: "PONG"}
	}
	return value.Value{Typ: "string", Str: args[0].Bulk}
}

func set(args []value.Value) value.Value {
	if len(args) != 2 {
		return value.Value{Typ: "error", Str: "ERR wrong number of arguments for the 'set' command"}
	}

	k := args[0].Bulk
	v := args[1].Bulk

	SETsMu.Lock()
	SETs[k] = v
	SETsMu.Unlock()

	return value.Value{Typ: "string", Str: "OK"}
}

func get(args []value.Value) value.Value {
	if len(args) != 1 {
		return value.Value{Typ: "error", Str: "ERR wrong number of arguments for the 'get' command"}
	}

	k := args[0].Bulk

	SETsMu.RLock()
	v, ok := SETs[k]
	SETsMu.RUnlock()

	if !ok {
		return value.Value{Typ: "null"}
	}

	return value.Value{Typ: "bulk", Bulk: v}
}

func hset(args []value.Value) value.Value {
	if len(args) != 3 {
		return value.Value{Typ: "error", Str: "ERR wrong number of arguments for the 'hset' command"}
	}

	h := args[0].Bulk
	k := args[1].Bulk
	v := args[2].Bulk

	HSETsMu.Lock()
	if _, ok := HSETs[h]; !ok {
		HSETs[h] = map[string]string{}
	}
	HSETs[h][k] = v
	HSETsMu.Unlock()

	return value.Value{Typ: "string", Str: "OK"}
}

func hget(args []value.Value) value.Value {
	if len(args) != 2 {
		return value.Value{Typ: "error", Str: "ERR wrong number of arguments for the 'hget' command"}
	}

	h := args[0].Bulk
	k := args[1].Bulk

	HSETsMu.RLock()
	v, ok := HSETs[h][k]
	HSETsMu.RUnlock()

	if !ok {
		return value.Value{Typ: "null"}
	}

	return value.Value{Typ: "bulk", Bulk: v}
}

func hgetall(args []value.Value) value.Value {
	if len(args) != 1 {
		return value.Value{Typ: "error", Str: "ERR wrong number of arguments for the 'hgetall' command"}
	}

	h := args[0].Bulk

	HSETsMu.RLock()
	v, ok := HSETs[h]
	HSETsMu.RUnlock()

	if !ok {
		return value.Value{Typ: "null"}
	}

	all := []value.Value{}

	for key, val := range v {
		all = append(all, value.Value{Typ: "bulk", Bulk: key})
		all = append(all, value.Value{Typ: "bulk", Bulk: val})
	}

	return value.Value{Typ: "array", Array: all}
}
