package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/ably/ably-go/ably"
	"github.com/go-redis/redis"
)

func getEnv(envName, valueDefault string) string {
	value := os.Getenv(envName)
	if value == "" {
		return valueDefault
	}
	return value
}

func getRedis() *redis.Client {
	var (
		host     = getEnv("REDIS_HOST", "localhost")
		port     = string(getEnv("REDIS_PORT", "6379"))
		password = getEnv("REDIS_PASSWORD", "")
	)

	client := redis.NewClient(&redis.Options{
		Addr:     host + ":" + port,
		Password: password,
		DB:       0,
	})
	_, err := client.Ping().Result()
	if err != nil {
		log.Fatal(err)
	}
	return client
}

var ctx = context.Background()

var wg = &sync.WaitGroup{}

func getAblyChannel() ably.RealtimeChannel {
	opts := &ably.ClientOptions{
		AuthOptions: ably.AuthOptions{
			Key: getEnv("ABLY_KEY", "No key specified"),
		},
	}
	ablyClient, err := ably.NewRealtimeClient(opts)
	if err != nil {
		panic(err)
	}
	return *ablyClient.Channels.Get(getEnv("CHANNEL_NAME", "channel-name"))

}

func transactionWithRedis(client *redis.Client, channel ably.RealtimeChannel) error {

	redisQueueName := getEnv("QUEUE_KEY", "myJobQueue")

	redisLogName := redisQueueName + ":log"

	now := time.Now().UnixNano()
	windowSize := int64(time.Second)
	clearBefore := now - windowSize

	rateLimit, _ := strconv.ParseInt(getEnv("RATE_LIMIT", "50"), 10, 64)

	err := client.Watch(func(tx *redis.Tx) error {

		tx.ZRemRangeByScore(redisLogName, "0", strconv.FormatInt(clearBefore, 10))

		messageThisSecond, err := tx.ZCard(redisLogName).Result()

		if err != nil && err != redis.Nil {
			return err
		}

		if messageThisSecond < rateLimit {
			err = tx.ZAdd(redisLogName, redis.Z{
				Score:  float64(now),
				Member: now,
			}).Err()
			if err != nil && err != redis.Nil {
				return err
			}
			messageToPublish, err := tx.BLPop(0*time.Second, redisQueueName).Result()
			if err != nil && err != redis.Nil {
				return err
			}

			_, err = channel.Publish("trade", messageToPublish[1])
			if err != nil {
				fmt.Println(err)
			}
		}
		return err
	}, redisLogName)

	return err

}

func main() {
	client := getRedis()
	channel := getAblyChannel()

	go func() {
		for {
			transactionWithRedis(client, channel)
		}
	}()

	wg.Add(1)
	wg.Wait()

}
