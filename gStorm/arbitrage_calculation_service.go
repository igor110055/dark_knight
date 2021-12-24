package main

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/go-redis/redis/v8"
)

func check_arbitrage(
	naturalSymbol string,
	synthetics *[2]*CryptoSymbol,
	redisClient *redis.Client) (float32, float32, error) {

	var err error

	for _, synthetic := range synthetics {
		synthetic.Prices, err = getBestPrice(synthetic.Symbol, redisClient)
		if err != nil {
			return 0.0, 0.0, err
		}
	}

	naturalPrices, err := getBestPrice(naturalSymbol, redisClient)
	if err != nil {
		return 0.0, 0.0, err
	}

	bssnReturn, err := buySyntheticSellNaturalReturn(naturalPrices.Bids, synthetics)
	if err != nil {
		return 0.0, 0.0, err
	}
	bnssReturn, err := buyNaturalSellSyntheticReturn(naturalPrices.Asks, synthetics)
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

func getBestPrice(symbol string, redisClient *redis.Client) (*BestPrice, error) {
	var bestPrice BestPrice
	var bestPriceStr string
	var err error

	bestPriceStr, err = redisClient.Get(ctx, fmt.Sprintf("best_prices:%s", symbol)).Result()
	// fmt.Println(bestPriceStr)
	if err != nil {
		return nil, err
	}

	err = json.NewDecoder(strings.NewReader(bestPriceStr)).Decode(&bestPrice)
	if err != nil {
		return nil, err
	}

	return &bestPrice, nil
}
