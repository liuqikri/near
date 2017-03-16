package util

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"log"
)

type SuperObject interface{}

func ParseJsonAndDecode(data SuperObject, args []string) error {
	fmt.Println(data)
	//base64解码
	arg, err := base64.StdEncoding.DecodeString(args[0])
	if err != nil {
		log.Println("ParseJson base64 decode error.")
		return err
	}
	log.Println("data after decode:" + string(arg[:]))
	fmt.Println("data after decode:" + string(arg[:]))

	//解析数据
	err = json.Unmarshal(arg, data)
	if err != nil {
		log.Println("ParseJson json Unmarshal error.")
		return err
	}
	fmt.Println("----------------------------------")
	fmt.Println(data)
	fmt.Println("Parse json is ok.")

	return nil
}

func ParseJson(data SuperObject, arg string) error {
	fmt.Println(data)
	//解析数据
	bytes := []byte(arg)
	err := json.Unmarshal(bytes, data)
	if err != nil {
		log.Println("ParseJson json Unmarshal error.")
		return err
	}
	fmt.Println("----------------------------------")
	fmt.Println(data)
	fmt.Println("Parse json is ok.")
	return nil
}
