package main

import (
	"fmt"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/gamelogic"
)

func main() {
	log := gamelogic.GetMaliciousLog()
	fmt.Printf("%T\n%+v\n", log, log)
}
