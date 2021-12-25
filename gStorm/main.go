package main

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/go-redis/redis/v8"
)

var ctx = context.Background()

var EXPECTED_RETURN float32 = 0.003

func main() {
	if len(os.Args) > 1 {
		expected_return, _ := strconv.ParseFloat(os.Args[1], 32)
		EXPECTED_RETURN = float32(expected_return)
	}

	fmt.Println("EXPECTED RETURN:", EXPECTED_RETURN)

	rdb := redis.NewClient(&redis.Options{
		Network: "unix",
		// Addr:     "localhost:6379",
		Addr:     "/var/run/redis/redis-server.sock",
		Password: "", // no password set
		DB:       0,  // use default DB
	})

	tradingGroups, err := readTradingGroup("../trading_groups.yml")
	if err != nil {
		fmt.Println(err)
	}

	bssnReturn := make(map[string]float32, len(*tradingGroups))
	bnssReturn := make(map[string]float32, len(*tradingGroups))
	var now time.Time

	for {
		for _, tradingGroup := range *tradingGroups {
			natural := tradingGroup.Natural
			synthetics := tradingGroup.Synthetic
			bssnReturn[natural.Symbol], bnssReturn[natural.Symbol], err = checkArbitrage(natural, synthetics, rdb)
			if err != nil {
				fmt.Println(err)
			} else {
				now = time.Now()
				if bssnReturn[natural.Symbol] > EXPECTED_RETURN {
					fmt.Println(natural.Symbol, now, "Buy synthetic sell natural", bssnReturn[natural.Symbol])
				}

				if bnssReturn[natural.Symbol] > EXPECTED_RETURN {
					fmt.Println(natural.Symbol, now, "Buy natural sell synthetic", bnssReturn[natural.Symbol])
				}
			}
		}

		time.Sleep(time.Millisecond)
	}
}
