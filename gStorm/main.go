package main

import (
	"context"
	"fmt"
	"time"

	"github.com/go-redis/redis/v8"
)

var ctx = context.Background()

const EXPECTED_RETURN = 0.003

func main() {
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

	var prevBssnReturn, prevBnssReturn, bssnReturn, bnssReturn float32

	for {
		for _, tradingGroup := range *tradingGroups {
			natural := tradingGroup.Natural.Symbol
			synthetics := tradingGroup.Synthetic
			bssnReturn, bnssReturn, err = check_arbitrage(natural, synthetics, rdb)
			if err == nil && bssnReturn != prevBssnReturn && bnssReturn != prevBnssReturn {
				if bssnReturn > EXPECTED_RETURN || bnssReturn > EXPECTED_RETURN {
					fmt.Println(natural, time.Now(), bssnReturn, bnssReturn)
					prevBssnReturn = bssnReturn
					prevBnssReturn = bnssReturn
				}
			}
		}
		time.Sleep(time.Millisecond)
	}
}
