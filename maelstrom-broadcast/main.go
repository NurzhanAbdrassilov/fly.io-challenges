package main

import (
	"encoding/json"
	"log"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

func main() {
	n := maelstrom.NewNode()
	hist := []int{}
	cur_topology := make(map[string][]string)

	n.Handle("broadcast", func(msg maelstrom.Message) error {
		// Unmarshal the message body as an loosely-typed map.
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		num_float, _ := body["message"].(float64)
		num := int(num_float)
		hist = append(hist, num)
		// Update the message type to return back.

		for _, neighbor := range cur_topology[n.ID()] {
			forward := map[string]any{
				"type":    "broadcast",
				"message": num,
			}
			n.Send(neighbor, forward)
		}

		body["type"] = "broadcast_ok"
		delete(body, "message")
		// Echo the original message back with the updated message type.
		return n.Reply(msg, body)
	})

	n.Handle("read", func(msg maelstrom.Message) error {
		// Unmarshal the message body as an loosely-typed map.
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		// Update the message type to return back.
		body["type"] = "read_ok"

		body["messages"] = hist

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
