package main

// Reading chunks of a file instead of line by line

import (
	"bufio"
	"bytes"
	"fmt"
	"log"
	"math"
	"os"
	"sync"
)

type It3City struct {
	Name     string
	MinTemp  float64
	MeanTemp float64
	MaxTemp  float64
	Count    int
	SumTemp  float64
}

func (c It3City) Print() {
	fmt.Printf("%s;%v;%v;%v\n", c.Name, c.MinTemp, c.MeanTemp, c.MaxTemp)
}

func (c It3City) PrintDebug() {
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

type It3CityList map[string]It3City

const (
	MAX_BUFFER_CAPACITY = 10 * 1024 * 1024 // 1MB, adjust as needed
	MAX_WORKERS         = 100              // adjust as needed
)

func It3Concurrent(filename string) error {
	file, err := os.Open(filename)
	if err != nil {
		return err
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	buf := make([]byte, MAX_BUFFER_CAPACITY)
	scanner.Buffer(buf, MAX_BUFFER_CAPACITY)
	var cities []It3CityList
	var wg sync.WaitGroup
	var mu sync.Mutex

	// Create a pool of worker goroutines
	jobs := make(chan []byte, MAX_WORKERS)

	for i := 0; i < MAX_WORKERS; i++ {
		wg.Add(1)
		go processChunk(jobs, &cities, &mu, &wg)
	}

	chunk := make([]byte, 0, MAX_BUFFER_CAPACITY)

	for scanner.Scan() {
		line := scanner.Bytes()
		if len(chunk)+len(line)+1 > MAX_BUFFER_CAPACITY {
			jobs <- append([]byte(nil), chunk...)
			chunk = chunk[:0] // Reset chunk
		}
		chunk = append(chunk, line...)
		chunk = append(chunk, '\n')
	}

	if len(chunk) > 0 {
		jobs <- chunk
	}

	if err := scanner.Err(); err != nil {
		return err
	}

	close(jobs)
	wg.Wait()

	fmt.Println("")
	fmt.Println("")
	log.Printf("Processed %v CityList\n", len(cities))
	// log.Printf("Processed %v unique cities\n", len(cities))

	cityHash := processCities(cities)
	for _, c := range cityHash {
		c.Print()
	}
	log.Printf("Processed %v unique cities\n", len(cityHash))
	return nil
}

func processChunk(
	jobs <-chan []byte,
	c *[]It3CityList,
	mu *sync.Mutex,
	wg *sync.WaitGroup,
) {
	defer wg.Done()

	cityHash := make(It3CityList)
	for chunk := range jobs {
		lines := bytes.Split(chunk, []byte{'\n'})

		if len(lines) > 0 {
			for i := 0; i < len(lines)-1; i++ {
				line := bytes.Split(lines[i], []byte{';'})
				if len(line) != 2 {
					fmt.Println("wrong line:", len(line))
					fmt.Printf("Lines: %v\n", string(lines[i]))
				}
				fileCity := string(line[0])
				tmp, err := CustomParseFloatFromString(string(line[1]))
				if err != nil {
					fmt.Printf("Lines: %v\n", string(lines[i]))
					log.Printf("error parsing float64. city: %v, err: %v\n", fileCity, err)
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
					// city.MeanTemp = math.Round(city.SumTemp/float64(city.Count)*100) / 100
					city.MeanTemp = 0
					cityHash[fileCity] = city
				} else {
					cityHash[fileCity] = It3City{
						Name:     fileCity,
						Count:    1,
						SumTemp:  tmp,
						MinTemp:  tmp,
						MaxTemp:  tmp,
						MeanTemp: tmp,
					}
				}
			}
		}
	}

	// log.Println("mutexing...")
	// return nil
	mu.Lock()
	defer mu.Unlock()
	*c = append(*c, cityHash)
	// log.Println("fin job")
}

func processCities(cities []It3CityList) It3CityList {
	finalMap := make(It3CityList)
	for _, c := range cities {
		if len(c) > 0 {
			for _, s := range c {
				city, exist := finalMap[s.Name]
				if exist {
					var minTemp, maxTemp, meanTemp float64
					cnt := city.Count + s.Count
					sumTemp := s.SumTemp + city.SumTemp
					if city.MinTemp > s.MinTemp {
						minTemp = s.MinTemp
					} else {
						minTemp = city.MinTemp
					}
					if city.MaxTemp < s.MaxTemp {
						maxTemp = s.MaxTemp
					} else {
						maxTemp = city.MaxTemp
					}
					meanTemp = math.Round(sumTemp/float64(cnt)*100) / 100
					finalMap[s.Name] = It3City{
						Name:     s.Name,
						Count:    cnt,
						SumTemp:  sumTemp,
						MinTemp:  minTemp,
						MaxTemp:  maxTemp,
						MeanTemp: meanTemp,
					}
				} else {
					finalMap[s.Name] = It3City{
						Name:     s.Name,
						Count:    s.Count,
						SumTemp:  s.SumTemp,
						MinTemp:  s.MinTemp,
						MaxTemp:  s.MaxTemp,
						MeanTemp: s.MeanTemp,
					}
				}
			}
		}
	}
	return finalMap
}
