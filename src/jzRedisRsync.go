package main

import (
	"flag"
	"fmt"
	"jz"
	"os"
)

var optionConfigFile = flag.String("config", "./config.xml", "configure xml file")

func usage() {
	fmt.Printf("Usage: %s [options]Options:", os.Args[0])
	flag.PrintDefaults()
	os.Exit(0)
}

func main() {
	flag.Usage = usage
	flag.Parse()

	if len(os.Args) < 2 {
		usage()
	}

	_, err := jz.ParseXmlConfig(*optionConfigFile)
	if err != nil {
		jz.JzLogger.Print(err)
		os.Exit(1)
	}

	jz.Run()
}
