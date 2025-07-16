package main

import (
	"encoding/json"
	"fmt"
	"log"
	"strconv"
	"sync"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

func main() {
	n := maelstrom.NewNode()
	hist := make(map[int]bool)
	cur_topology := make(map[string][]string)
	var lock sync.Mutex

	n.Handle("broadcast", func(msg maelstrom.Message) error {
		// Unmarshal the message body as an loosely-typed map.
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		num_float, _ := body["message"].(float64)
		num := int(num_float)
		lock.Lock()
		hist[num] = true
		lock.Unlock()

		for _, neighbor := range cur_topology[n.ID()] {
			gossip := map[string]any{
				"type":    "gossip",
				"message": float64(num),
				"TTL":     float64(5),
			}

			n.Send(neighbor, gossip)
		}

		body["type"] = "broadcast_ok"
		delete(body, "message")
		// Echo the original message back with the updated message type.
		return n.Reply(msg, body)
	})

	n.Handle("gossip", func(msg maelstrom.Message) error {
		// Unmarshal the message body as an loosely-typed map.
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		num_float, _ := body["message"].(float64)
		num := int(num_float)
		lock.Lock()
		hist[num] = true
		lock.Unlock()

		var ttl int
		switch v := body["TTL"].(type) {
		case float64:
			ttl = int(v)
		case string:
			parsed, err := strconv.Atoi(v)
			if err != nil {
				return err
			}
			ttl = parsed
		default:
			return fmt.Errorf("wrong TTL type: %T", v)
		}

		ttl -= 1
		if ttl != 0 {
			for _, neighbor := range cur_topology[n.ID()] {
				gossip := map[string]any{
					"type":    "gossip",
					"message": float64(num),
					"TTL":     float64(ttl),
				}

				n.Send(neighbor, gossip)
			}
		}
		return nil
	})

	n.Handle("read", func(msg maelstrom.Message) error {
		// Unmarshal the message body as an loosely-typed map.
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		// Update the message type to return back.
		body["type"] = "read_ok"

		lock.Lock()
		arr := make([]int, 0, len(hist))
		for k := range hist {
			arr = append(arr, k)
		}
		lock.Unlock()
		body["messages"] = arr

		// Echo the original message back with the updated message type.
		return n.Reply(msg, body)
	})

	n.Handle("topology", func(msg maelstrom.Message) error {
		// Unmarshal the message body as an loosely-typed map.
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		// Update the message type to return back.
		body["type"] = "topology_ok"

		topology_raw := body["topology"].(map[string]any)
		new_topology := make(map[string][]string)

		for k, v := range topology_raw {
			neighbors_any := v.([]any)
			neighbors := make([]string, len(neighbors_any))
			for i, neighbor := range neighbors_any {
				neighbors[i] = neighbor.(string)
			}
			new_topology[k] = neighbors
		}

		cur_topology = new_topology

		delete(body, "topology")
		// Echo the original message back with the updated message type.
		return n.Reply(msg, body)
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}

}
