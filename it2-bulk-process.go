package main

import (
	"bufio"
	"fmt"
	"log"
	"math"
	"os"
	"strconv"
	"strings"
	"sync"
)

// Scanner first gathers a number of lines
// then sends for processing...

const CHUNK_LINES = 1000000

type It2City struct {
	Name     string
	MinTemp  float64
	MeanTemp float64
	MaxTemp  float64
	Count    int
	SumTemp  float64
}

func (c It2City) Print() {
	fmt.Printf("%s;%v;%v;%v\n", c.Name, c.MinTemp, c.MeanTemp, c.MaxTemp)
}

func (c It2City) PrintDebug() {
	fmt.Printf(
		"Name:%s; Min:%v; Mean:%v; Max:%v; Count:%v; Sum:%v\n",
		c.Name,
		c.MinTemp,
		c.MeanTemp,
		c.MaxTemp,
		c.Count,
		c.SumTemp,
	)
}

type It2CityList map[string]It2City

type It2CityMap struct {
	mu     sync.Mutex
	cities It2CityList
}

func (c *It2CityMap) Get(key string) (It2City, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	val, ok := c.cities[key]
	return val, ok
}

func (c *It2CityMap) New(city It2City) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.cities[city.Name] = city
	return nil
}

func (c *It2CityMap) Update(city It2City) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.cities[city.Name] = city
	return nil
}

func It2BulkProcess(filename string) {
	log.Println("1BRC Bulk Read, then Bulk Process")

	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("failure opening file: %v\n", err)
	}
	defer file.Close()

	lc := 0
	cnt := 0
	var wg sync.WaitGroup
	cityMap := It2CityMap{cities: make(map[string]It2City)}
	var chunk []string

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		lc++
		if cnt == CHUNK_LINES {
			wg.Add(1)
			go processChunks(&wg, &cityMap, chunk)
			// go processDummy(&wg, &cityMap, chunk)

			// reset items
			// clear(chunk)
			chunk = []string{}
			cnt = 0
			continue
		}
		line := scanner.Text()
		cnt++
		chunk = append(chunk, line)
	}
	wg.Wait()

	if err := scanner.Err(); err != nil {
		log.Fatalf("Scanner error:", err)
	}

	// log.Printf(`Cities: %v`, cityHash)
	for _, c := range cityMap.cities {
		c.Print()
	}

	fmt.Println("")
	fmt.Println("")
	log.Printf("Processed %v entries\n", lc)
	log.Printf("Processed %v unique cities\n", len(cityMap.cities))
	log.Println("Done")
}

func processChunks(
	wg *sync.WaitGroup,
	cMap *It2CityMap,
	chunk []string,
) error {
	// log.Println("process chunk start...")
	defer wg.Done()
	for _, line := range chunk {
		parts := strings.SplitN(line, ";", 2)
		fileCity := parts[0]
		tmp, err := strconv.ParseFloat(parts[1], 64)
		if err != nil {
			log.Printf("error parsing float64. city: %v, err: %v\n", parts[0], err)
		}
		city, exist := cMap.Get(fileCity)
		if exist {
			var sumTmp, minTmp, maxTmp, meanTmp float64

			sumTmp = city.SumTemp + tmp
			if tmp < city.MinTemp {
				minTmp = tmp
			} else {
				minTmp = city.MinTemp
			}

			if tmp > city.MaxTemp {
				maxTmp = tmp
			} else {
				maxTmp = city.MaxTemp
			}

			meanTmp = math.Round(sumTmp/float64(city.Count+1)*100) / 100
			//
			cMap.Update(It2City{
				Name:     fileCity,
				Count:    city.Count + 1,
				SumTemp:  city.SumTemp + tmp,
				MinTemp:  minTmp,
				MaxTemp:  maxTmp,
				MeanTemp: meanTmp,
			})
		} else {
			cMap.New(It2City{
				Name:     fileCity,
				Count:    1,
				SumTemp:  tmp,
				MinTemp:  tmp,
				MaxTemp:  tmp,
				MeanTemp: tmp,
			})
		}
	}

	// log.Println("process chunk end...")
	return nil
}
