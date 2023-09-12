package main

import (
	"context"
	"embed"
	"fmt"
	"github.com/redis/go-redis/v9"
	"sync"
	"time"
)

//go:embed wrr.lua
var fs embed.FS

func main() {
	loadBalance()
}

func loadBalance() {
	client := redis.NewClient(&redis.Options{Addr: "localhost:6379"})
	key := "wrr"
	w, err := NewWrr(client, key, []Server{
		{
			Addr:   "https://www.baidu.com",
			Name:   "baidu",
			Weight: 5,
		},
		{
			Addr:   "https://www.taobao.com",
			Name:   "taobao",
			Weight: 2,
		},
	})
	if err != nil {
		panic(err)
	}
	var wg sync.WaitGroup
	for i := 0; i < 7; i++ {
		wg.Add(1)
		go func(i int) {
			no := time.Now()
			nex, _ := w.Next()
			fmt.Println(i, nex, time.Since(no))
			wg.Done()
		}(i)
	}
	wg.Wait()
}

type Wrr struct {
	cli     *redis.Client
	key     string
	servers []Server
	script  string
}

type Server struct {
	Addr   string
	Name   string
	Weight int
}

func NewWrr(cli *redis.Client, key string, servers []Server) (*Wrr, error) {
	file, err := fs.ReadFile("wrr.lua")
	if err != nil {
		return nil, err
	}
	w := &Wrr{
		cli:     cli,
		key:     key,
		script:  string(file),
		servers: servers,
	}
	hss := make([]interface{}, 0)
	zs := make([]redis.Z, 0)
	for _, s := range servers {
		hss = append(hss, s.Name+"_weight", s.Weight)
		hss = append(hss, s.Name+"_addr", s.Addr)
		zs = append(zs, redis.Z{
			Member: s.Name,
		})
	}
	err = cli.Del(context.Background(), key+"_meta", key+"_servers").Err()
	if err != nil {
		return nil, err
	}
	err = cli.HMSet(context.Background(), key+"_meta", hss...).Err()
	if err != nil {
		return nil, err
	}
	err = cli.ZAdd(context.Background(), key+"_servers", zs...).Err()
	return w, err
}

func (w *Wrr) Next() (string, error) {
	eval := w.cli.Eval(context.Background(), w.script,
		[]string{w.key + "_meta", w.key + "_servers"})
	return eval.Text()
}
