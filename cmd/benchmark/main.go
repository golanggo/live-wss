package main

import (
	"flag"
	"fmt"
	"os"

	"live-wss/test"
)

func main() {
	// 命令行参数
	mode := flag.String("mode", "quick", "压测模式: quick(快速), normal(常规), redis(Redis), high(高负载), redis10k(1万观众Redis)")
	flag.Parse()

	fmt.Println("========================================")
	fmt.Println("       直播间系统压力测试工具")
	fmt.Println("========================================")
	fmt.Printf("运行模式: %s\n", *mode)
	fmt.Println("")

	switch *mode {
	case "quick":
		fmt.Println("执行快速压测 (100观众, 50消息/人)")
		test.RunQuickBenchmark()
	case "normal":
		fmt.Println("执行常规压测 (10000观众, 10消息/人)")
		test.RunBenchmark()
	case "redis":
		fmt.Println("执行Redis压测 (5000观众, 20消息/人)")
		test.RunBenchmarkWithRedis()
	case "high":
		fmt.Println("执行高负载压测 (40000观众, 5消息/人)")
		test.RunHighLoadBenchmark()
	case "redis10k":
		fmt.Println("执行Redis压测 (10000观众, 10消息/人)")
		test.RunRedisBenchmarkWith10KViewers()
	default:
		fmt.Printf("未知模式: %s\n", *mode)
		fmt.Println("可用模式: quick, normal, redis, high, redis10k")
		os.Exit(1)
	}
}
