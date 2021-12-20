package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"strings"
	"time"

	"gopkg.in/yaml.v3"

	"github.com/go-redis/redis/v8"
)

var ctx = context.Background()

func main() {
	rdb := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
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
			bssnReturn, bnssReturn, err = check_arbitrage(natural, synthetics, 0.0002, rdb)
			if err == nil && bssnReturn != prevBssnReturn && bnssReturn != prevBnssReturn {
				if bssnReturn > 0.0 || bnssReturn > 0.0 {
					fmt.Println(natural, time.Now(), bssnReturn, bnssReturn)
					prevBssnReturn = bssnReturn
					prevBnssReturn = bnssReturn
				}
			}
		}
	}
}

type BestPrice struct {
	Bids float32 `json:"bids"`
	Asks float32 `json:"asks"`
}

type TradingGroup struct {
	Natural   *CryptoSymbol     `yaml:"natural"`
	Synthetic *[2]*CryptoSymbol `yaml:"synthetic"`
}

type CryptoSymbol struct {
	Symbol string     `yaml:"symbol"`
	Normal bool       `yaml:"normal"`
	Assets *[2]string `yaml:"assets"`
	Prices *BestPrice
}

func check_arbitrage(
	naturalSymbol string,
	synthetics *[2]*CryptoSymbol,
	targetReturn float32,
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

func readTradingGroup(filename string) (*[]TradingGroup, error) {
	buf, err := ioutil.ReadFile(filename)
	if err != nil {
		return nil, err
	}

	tradingGroups := &[]TradingGroup{}
	err = yaml.Unmarshal(buf, tradingGroups)
	if err != nil {
		return nil, err
	}

	return tradingGroups, nil
}
