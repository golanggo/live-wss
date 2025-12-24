package redis

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

var (
	RDB  *redis.ClusterClient
	ctx  = context.Background()
	once sync.Once
)

func InitRedis() error {
	var initErr error
	once.Do(func() {
		err := connect()
		if err != nil {
			initErr = err
		} else {
			fmt.Sprint("Redis cluster connected")
		}

		go reconnectionLoop()
	})

	return initErr
}

func connect() error {
	addr := getRedisAddrs()
	fmt.Sprintf("Redis cluster nodes: %v", addr)

	client := redis.NewClusterClient(&redis.ClusterOptions{
		Addrs:            addr,
		Password:         "",
		PoolSize:         200,
		MinIdleConns:     200,
		DialTimeout:      10 * time.Second,
		ReadTimeout:      3 * time.Second,
		WriteTimeout:     3 * time.Second,
		ConnMaxIdleTime:  60 * time.Second,
		ReadOnly:         false,
		RouteByLatency:   true,
		MaxRedirects:     5, //  MaxRedirects设5 可在 异常抖动 时仍保住请求，又避免无限循环
		MaxRetries:       3, // 网络闪断时自动重试
		MinRetryBackoff:  8 * time.Millisecond,
		MaxRetryBackoff:  512 * time.Millisecond,
		ClientName:       "live_wss",
		DisableIndentity: true, // go-redisV9 默认会在每次新建连接时 向服务器发送两条 Redis-7 才支持的子命令,加这个参数，忽略掉
	})
	if _, err := client.Ping(ctx).Result(); err != nil {
		return err
	}
	fmt.Sprint("Redis cluster connected")
	RDB = client
	return nil
}

func reconnectionLoop() {
	ticker := time.NewTicker(30 * time.Second) // 每30秒检查一次连接
	defer ticker.Stop()

	for range ticker.C {
		// 如果RDB为nil，尝试重新连接
		if RDB == nil {
			fmt.Sprint("Redis client is nil, attempting to reconnect...")
			if err := connect(); err != nil {
				fmt.Errorf("Redis reconnection failed: %v", err)
			} else {
				fmt.Sprint("Redis reconnection successful")
			}
		} else {
			// 检查连接是否正常
			if err := RDB.Ping(ctx).Err(); err != nil {
				fmt.Errorf("Redis ping failed: %v, attempting to reconnect...", err)
				// 先关闭现有连接
				if err := RDB.Close(); err != nil {
					fmt.Errorf("Error closing Redis connection: %v", err)
				}
				if err := connect(); err != nil {
					fmt.Errorf("Redis reconnection failed: %v", err)
				} else {
					fmt.Sprint("Redis reconnection successful")
				}
			}
		}
	}
}

func getRedisAddrs() []string {
	addrs := []string{}
	return addrs

}
