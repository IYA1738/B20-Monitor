package app

import "log"

func main() {
	if err := app.Run(); err != nil {
		log.Fatal(err)
	}
}
