package main

import (
	"bufio"
	"fmt"
	"log"
	"math"
	"os"
	"strconv"
	"strings"
)

type It1City struct {
	Name     string
	MinTemp  float64
	MeanTemp float64
	MaxTemp  float64
	Count    int
	SumTemp  float64
}

func (c It1City) Print() {
	fmt.Printf("%s;%v;%v;%v\n", c.Name, c.MinTemp, c.MeanTemp, c.MaxTemp)
}

func It1SimpleImplementation(filename string) {
	log.Println("Starting 1brc")

	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("Failure to open file: %v", err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)

	cityHash := make(map[string]It1City)

	cnt := 0
	for scanner.Scan() {
		cnt++
		line := scanner.Text()
		parts := strings.SplitN(line, ";", 2)
		fileCity := parts[0]
		tmp, err := strconv.ParseFloat(parts[1], 64)
		if err != nil {
			log.Printf("error parsing float64. city: %v, err: %v\n", parts[0], err)
		}
		city, exist := cityHash[fileCity]
		if exist {
			city.Count++
			city.SumTemp += tmp
			if tmp < city.MinTemp {
				city.MinTemp = tmp
			}
			if tmp > city.MaxTemp {
				city.MaxTemp = tmp
			}
			city.MeanTemp = math.Round(city.SumTemp/float64(city.Count)*100) / 100
			cityHash[fileCity] = city
		} else {
			cityHash[fileCity] = It1City{
				Name:     fileCity,
				Count:    1,
				SumTemp:  tmp,
				MinTemp:  tmp,
				MaxTemp:  tmp,
				MeanTemp: tmp,
			}
		}
		// log.Println("Line is", cityName, tmp)
	}

	if err := scanner.Err(); err != nil {
		log.Fatalf("Scanner error:", err)
	}

	// log.Printf(`Cities: %v`, cityHash)
	for _, c := range cityHash {
		c.Print()
	}

	fmt.Println("")
	fmt.Println("")
	log.Printf("Processed %v entries\n", cnt)
	log.Printf("Processed %v unique cities\n", len(cityHash))
}
