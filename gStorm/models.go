package main

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
