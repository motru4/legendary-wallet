package main

import (
	"log"
	"operation-worker/internal/app"
)

func main() {
	a, err := app.New()
	if err != nil {
		log.Fatal("error creating an application instance: ", err)
	}

	a.Run()
}
