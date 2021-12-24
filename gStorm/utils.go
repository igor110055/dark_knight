package main

import (
	"io/ioutil"

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
