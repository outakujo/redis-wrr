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
	w, err := NewBalancer(client, key, []Server{
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

type Balancer struct {
	cli      *redis.Client
	key      string
	servers  []Server
	script   string
	scriptId string
}

type Server struct {
	Addr   string
	Name   string
	Weight int
}

func NewBalancer(cli *redis.Client, key string, servers []Server) (*Balancer, error) {
	file, err := fs.ReadFile("wrr.lua")
	if err != nil {
		return nil, err
	}
	b := &Balancer{
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
	if err != nil {
		return nil, err
	}
	b.scriptId, err = cli.ScriptLoad(context.Background(), b.script).Result()
	return b, err
}

func (r *Balancer) Next() (next Server, err error) {
	sli, err := r.cli.EvalSha(context.Background(), r.scriptId,
		[]string{r.key + "_meta", r.key + "_servers"}).Slice()
	if err != nil {
		return
	}
	next.Addr = sli[0].(string)
	next.Name = sli[1].(string)
	next.Weight = int(sli[2].(int64))
	return
}
