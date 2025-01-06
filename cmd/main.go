package main

import (
	"data-replication/config"
	"log"
)

func main() {
	_, err := config.InitConfig()
	if err != nil {
		log.Fatal(err)
	}
}
