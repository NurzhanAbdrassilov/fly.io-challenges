package main

import (
	"encoding/json"
	"log"
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
		arr := make([]int, 0, len(hist))
		for k := range hist {
			arr = append(arr, k)
		}
		lock.Unlock()

		for _, neighbor := range cur_topology[n.ID()] {
			gossip := map[string]any{
				"type":    "gossip",
				"message": arr,
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

		arr_any := body["message"].([]any)
		var unseen []int
		lock.Lock()
		for _, v := range arr_any {
			num := int(v.(float64))
			if hist[num] == false {
				unseen = append(unseen, num)
				hist[num] = true
			}
		}
		lock.Unlock()

		if len(unseen) == 0 {
			return nil
		}

		for _, neighbor := range cur_topology[n.ID()] {
			gossip := map[string]any{
				"type":    "gossip",
				"message": unseen,
			}

			n.Send(neighbor, gossip)
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
