package main

import (
	//"crypto/sha256"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"log"

	//"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

func main() {
	n := maelstrom.NewNode()
	//var cnt int = 0
	//var id_str string = n.ID()
	//h := sha256.New()
	n.Handle("generate", func(msg maelstrom.Message) error {
		// Unmarshal the message body as an loosely-typed map.
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		// Update the message type to return back.
		body["type"] = "generate_ok"

		//h.Reset()
		//timestamp := time.Now().UnixNano()
		//h.Write([]byte(timestamp + "/" + id_str + "%" + strconv.Itoa(cnt)))
		//hashed := h.Sum(nil)
		rand_num := make([]byte, 16) // 8 random bytes
		rand.Read(rand_num)
		//body["id"] = strconv.FormatInt(timestamp, 10) + "/" + id_str + "%" + string(rand_num)
		body["id"] = hex.EncodeToString(rand_num)
		//cnt += 1
		// Echo the original message back with the updated message type.
		return n.Reply(msg, body)
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
