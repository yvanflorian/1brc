package main

import (
	"fmt"
	"io"
	"log"
	"math"
	"os"
	"strings"
	"sync"
)

// everything prefixed with RC
const (
	RC_MAX_BUFFER_CAPACITY = 1 * 1024 * 1024
	RC_MAX_WORKERS         = 1000
)

type It4City struct {
	Name     string
	MinTemp  float64
	MeanTemp float64
	MaxTemp  float64
	Count    int
	SumTemp  float64
}

func (c It4City) Print() {
	fmt.Printf("%s;%v;%v;%v\n", c.Name, c.MinTemp, c.MeanTemp, c.MaxTemp)
}

type It4CityList map[string]It4City

// main function
func It4ReadChunks(filename string) error {
	file, err := os.Open(filename)
	if err != nil {
		return err
	}
	defer file.Close()

	buf := make([]byte, RC_MAX_BUFFER_CAPACITY)
	iteration := 0

	var cities []It4CityList
	var wg, wg2 sync.WaitGroup

	// var mu sync.Mutex
	jobs := make(chan []byte, RC_MAX_WORKERS)
	cityChan := make(chan It4CityList, RC_MAX_WORKERS)

	for i := 0; i < RC_MAX_WORKERS; i++ {
		wg.Add(1)
		// go RC_processchunks(jobs, &cities, &mu, &wg)
		go RC_processchunks(jobs, &wg, cityChan)
	}

	wg2.Add(1)
	go RC_resultCollector(cityChan, &cities, &wg2)

	// read file
	for {
		iteration++
		var chunk []byte
		n, err := io.ReadFull(file, buf)
		if err != nil {
			if err == io.EOF {
				break
			} else if err == io.ErrUnexpectedEOF {
				// This occurs when the file is shorter than the buffer
				// and EOF is reached before filling the buffer
				chunk = append(chunk, buf[:n]...)
				if len(chunk) > 0 {
					jobs <- chunk
				}
				break
			}
		}
		chunk = append(chunk, buf[:n]...)
		if buf[n-1] != '\n' {
			var additionalBytes []byte
			for {
				// Create a 1-byte buffer to read one byte at a time
				nextByte := make([]byte, 1)

				_, err := file.Read(nextByte)
				if err != nil {
					if err == io.EOF {
						break // End of file, stop reading
					}
					log.Fatalf("Error reading until newline: %v", err)
				}

				// Append the byte to the additionalBytes slice
				additionalBytes = append(additionalBytes, nextByte[0])

				// Stop reading if we encounter a newline
				if nextByte[0] == '\n' {
					break
				}
			}
			chunk = append(chunk, additionalBytes...)
		}

		if len(chunk) > 0 {
			jobs <- chunk
		}
	}

	close(jobs)
	wg.Wait()

	close(cityChan)
	wg2.Wait()

	fmt.Println("")
	fmt.Println("")
	log.Printf("Processed %v It4CityList\n", len(cities))
	// log.Printf("Processed %v unique cities\n", len(cities))

	cityHash := ProcessIt4Cities(cities)
	for _, c := range cityHash {
		c.Print()
	}
	log.Printf("Processed %v unique cities\n", len(cityHash))

	return nil
}

func CustomParseFloatFromString(s string) (float64, error) {
	var result float64
	var sign, factor float64 = 1, 1
	var decimalFound bool

	for _, char := range s {
		switch {
		case char == '-':
			sign = -1
		case char == '.':
			decimalFound = true
		case char >= '0' && char <= '9':
			if decimalFound {
				factor /= 10
				result += float64(char-'0') * factor
			} else {
				result = result*10 + float64(char-'0')
			}
		default:
			return 0, fmt.Errorf("invalid character: %c", char)
		}
	}
	return sign * result, nil
}

func RC_processchunks(
	jobs <-chan []byte,
	wg *sync.WaitGroup,
	cityChan chan<- It4CityList,
) {
	defer wg.Done()
	cityHash := make(It4CityList)
	for chunk := range jobs {
		cn := string(chunk)
		for {
			part, left, found := strings.Cut(cn, "\n")
			if !found {
				break
			}
			RC_processPart(part, &cityHash)
			cn = left
		}
	}
	cityChan <- cityHash
}

func RC_processPart(s string, cityHash *It4CityList) {
	fileCity, strTmp, found := strings.Cut(s, ";")
	if !found {
		log.Fatalf("wrong string:", s)
	}

	// fileCity := line[0]
	tmp, err := CustomParseFloatFromString(strTmp)
	if err != nil {
		fmt.Printf("part: %v\n", s)
		log.Printf("error parsing float64. city: %v, err: %v\n", fileCity, err)
	}
	// fmt.Printf("Station: %s, Temp: %v\n", fileCity, tmp)
	city, exist := (*cityHash)[fileCity]
	if exist {
		city.Count++
		city.SumTemp += tmp
		city.MinTemp = min(city.MinTemp, tmp)
		city.MaxTemp = max(city.MaxTemp, tmp)
		// city.MeanTemp = math.Round(city.SumTemp/float64(city.Count)*100) / 100
		city.MeanTemp = 0
		(*cityHash)[fileCity] = city
	} else {
		(*cityHash)[fileCity] = It4City{
			Name:     fileCity,
			Count:    1,
			SumTemp:  tmp,
			MinTemp:  tmp,
			MaxTemp:  tmp,
			MeanTemp: tmp,
		}
	}
}

func RC_resultCollector(
	cities <-chan It4CityList,
	c *[]It4CityList,
	wg *sync.WaitGroup,
) {
	defer wg.Done()
	for city := range cities {
		*c = append(*c, city)
	}
}

func ProcessIt4Cities(cities []It4CityList) It4CityList {
	finalMap := make(It4CityList)
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
					finalMap[s.Name] = It4City{
						Name:     s.Name,
						Count:    cnt,
						SumTemp:  sumTemp,
						MinTemp:  minTemp,
						MaxTemp:  maxTemp,
						MeanTemp: meanTemp,
					}
				} else {
					finalMap[s.Name] = It4City{
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
