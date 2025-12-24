package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/golanggo/live-wss/test"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Println("用法: go run main.go [命令] [选项]")
		fmt.Println("")
		fmt.Println("命令:")
		fmt.Println("  test         运行压测")
		fmt.Println("  debug        运行调试测试")
		os.Exit(1)
	}

	cmd := os.Args[1]
	switch cmd {
	case "test":
		runTest()
	case "debug":
		fmt.Println("运行调试测试...")
		// 这里可以添加调试测试逻辑
	default:
		fmt.Printf("未知命令: %s\n", cmd)
		os.Exit(1)
	}
}

func runTest() {
	fs := flag.NewFlagSet("test", flag.ExitOnError)
	mode := fs.String("mode", "normal", "压测模式: simple-redis(简单Redis), multi-redis(Redis多房间)")
	fs.Parse(os.Args[2:])

	fmt.Println("========================================")
	fmt.Println("       直播间系统压力测试工具")
	fmt.Println("========================================")
	fmt.Printf("运行模式: %s\n", *mode)
	fmt.Println("")

	switch *mode {
	case "simple-redis":
		fmt.Println("执行简单Redis压测 (1房间, 500观众, 50消息/人)")
		test.RunSimpleRedisBenchmark()
	case "multi-redis":
		fmt.Println("执行Redis多房间压测 (10房间, 每房500观众)")
		test.RunMultiRoomBenchmarkWithRedis()
	default:
		fmt.Printf("未知模式: %s\n", *mode)
		fmt.Println("可用模式: simple-redis, multi-redis")
		os.Exit(1)
	}
}
