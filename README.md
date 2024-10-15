
# 1 Billion Row Challenge
This is a work in progress.

Had to attempt this [1BRC](https://1brc.dev/) fun challenge and see where it gets me.
Decided to use Go -- because, why not?
My personal challenge is to not use chatgpt or read anything about this challenge before I try it myself.


## Iterations
So far, I've had several iterations:

1. Basic Implementation:
Without thinking about concurrency, write the simplest solution to this challenge.
The code is in the `it1-simple.go`
**Result**: 123 seconds

2. Bulk Process a chunk of lines:
In this iteration, I read the file line by line, collect a given number of lines (in this case 1 million)
then lunch a go routine to process this chunk as the process collects the next one million of lines.
The code is in the `it2-bulk-process.go`
**Result**: 564 seconds -- which turned out to be more than the basic implementation.
Note: Acquiring the lock and releasing it that many times turned out to be a performance bottleneck.

3. Concurrency:
For this iteration, I'm adding an initial buffer size for bufio scanning.
Im adding in buffered channels and worker pools.
I bring out a number of worker pools (Adjusted to 100) which will process chunks of data as they're made available in the channels.
Also, I'm avoiding to acquire the lock to write to the common hash map in each goroutine, but rather do it at the end as you can see
in the file `it3-concurrent.go`
**Result**: 31seconds -- A huge improvement, but still not great.

4. Read Chunks of the file instead of line by line:
The main point is to now read chunks of the file instead of line by line.
I decided to not use the `bufio.scanner` as I realize through tests, that it doesn't guarantee to fill up the buffer size you set.
I also added in couple of optimizations based on the CPU profiling data:
- Made a custom Float parser
- Avoiding bytes.Split, strings.split (String.Cut turns out to be better)
- Introducing a result collector channel

**Result**: 12.09 seconds -- Things are looking good and really getting interesting
