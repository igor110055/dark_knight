package main

import (
	"fmt"

	"github.com/go-redis/redis/v8"
)

func checkArbitrage(
	natural *CryptoSymbol,
	synthetics *[2]*CryptoSymbol,
	redisClient *redis.Client) (float32, float32, error) {

	var err error

	err = getTradingPrices(natural, synthetics, redisClient)

	if err != nil {
		return 0.0, 0.0, err
	}

	bssnReturn, err := buySyntheticSellNaturalReturn(natural.Prices.Bids, synthetics)
	if err != nil {
		return 0.0, 0.0, err
	}
	bnssReturn, err := buyNaturalSellSyntheticReturn(natural.Prices.Asks, synthetics)
	if err != nil {
		return 0.0, 0.0, err
	}

	return bssnReturn, bnssReturn, nil
}

func buySyntheticSellNaturalReturn(naturalBid float32, synthetics *[2]*CryptoSymbol) (float32, error) {
	syntheticAsk, err := calculateSyntheticAsk(synthetics)
	if syntheticAsk == 0.0 || err != nil {
		return 0.0, fmt.Errorf("missing synthetic ask")
	}

	return (naturalBid - syntheticAsk) / syntheticAsk, nil
}

func buyNaturalSellSyntheticReturn(naturalAsk float32, synthetics *[2]*CryptoSymbol) (float32, error) {
	syntheticBid, err := calculateSyntheticBid(synthetics)
	if syntheticBid == 0.0 || err != nil {
		return 0.0, fmt.Errorf("missing synthetic bid")
	}

	return (syntheticBid - naturalAsk) / naturalAsk, nil
}

func calculateSyntheticAsk(synthetics *[2]*CryptoSymbol) (float32, error) {
	var syntheticAsks [2]float32
	var syntheticBid float32

	for index, synthetic := range synthetics {
		if synthetic.Normal {
			syntheticAsks[index] = synthetic.Prices.Asks
		} else {
			syntheticBid = synthetic.Prices.Bids
			if syntheticBid == 0.0 {
				return syntheticBid, fmt.Errorf("missing %v bid price", synthetic.Symbol)
			}

			syntheticAsks[index] = 1 / syntheticBid
		}
	}

	return syntheticAsks[0] * syntheticAsks[1], nil
}

func calculateSyntheticBid(synthetics *[2]*CryptoSymbol) (float32, error) {
	var syntheticBids [2]float32
	var syntheticAsk float32

	for index, synthetic := range synthetics {
		if synthetic.Normal {
			syntheticBids[index] = synthetic.Prices.Bids
		} else {
			syntheticAsk = synthetic.Prices.Asks
			if syntheticAsk == 0.0 {
				return syntheticAsk, fmt.Errorf("missing %v ask price", synthetic.Symbol)
			}

			syntheticBids[index] = 1 / syntheticAsk
		}
	}

	return syntheticBids[0] * syntheticBids[1], nil
}
