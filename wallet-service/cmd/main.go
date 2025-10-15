package main

import (
	"log"
	"wallet-service/internal/app"
)

func main() {
	a, err := app.New()
	if err != nil {
		log.Fatal("error creating an application instance: ", err)
	}

	err = a.Run()
	if err != nil {
		log.Fatal("application startup error: ", err)
	}
}
