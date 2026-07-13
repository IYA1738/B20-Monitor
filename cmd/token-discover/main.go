package main

import (
	"log"

	"token-discover-demo/cmd/app"
)

func main() {
	if err := app.Run(); err != nil {
		log.Fatal(err)
	}
}
