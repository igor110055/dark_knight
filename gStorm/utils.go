package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"strings"

	"github.com/go-redis/redis/v8"
	"gopkg.in/yaml.v3"
)

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

func getTradingPrices(natural *CryptoSymbol, synthetics *[2]*CryptoSymbol, redisClient *redis.Client) error {
	symbols := &[3]*CryptoSymbol{natural, synthetics[0], synthetics[1]}

	keys := symbolsToKeys(symbols)
	prices, err := redisClient.MGet(ctx, (*keys)...).Result()
	if err != nil {
		return err
	}

	for index, priceStr := range prices {
		json.NewDecoder(strings.NewReader(fmt.Sprintf("%v", priceStr))).Decode(&(symbols[index].Prices))
	}
	return nil
}

func symbolsToKeys(symbols *[3]*CryptoSymbol) *[]string {
	var keys []string

	for _, symbol := range *symbols {
		keys = append(keys, fmt.Sprintf("best_prices:%s", symbol.Symbol))
	}

	return &keys
}
