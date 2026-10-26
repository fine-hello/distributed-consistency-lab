package main

import (
	"encoding/csv"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

// Value 表示一个可复制的数据版本。Clock 是 Lamport 写入时间戳，Origin 是产生该版本的节点 ID。
// 注意：Clock 不能单独判断两个写入是否真正并发。
type Value struct {
	Data   string
	Clock  uint64
	Origin int
}

// Envelope 是节点间传递的消息；Ack 只表示接收节点已处理消息，不代表全局提交。
type Envelope struct {
	From, To int
	Key      string
	Value    Value
	Ack      chan struct{}
}

// Node 是一个模拟的分布式副本。每个节点由独立 Goroutine 处理 Inbox。
// mu 保护 clock/store：本地写操作和消息处理可能同时发生。
type Node struct {
	ID    int
	Inbox chan Envelope
	mu    sync.RWMutex
	clock uint64
	store map[string]Value
	mode  string // "version" or "arrival"
}

// newNode 创建节点；mode=arrival 时最后到达的消息覆盖已有值，
// mode=version 时按 (Clock, Origin) 比较版本。
func newNode(id int, mode string) *Node {
	return &Node{ID: id, Inbox: make(chan Envelope, 128), store: make(map[string]Value), mode: mode}
}

// newer 定义确定性的版本全序：先比较逻辑时钟，再以节点 ID 打破平局。
// 所有节点使用相同的比较规则，才可能在乱序情况下收敛。
func newer(a, b Value) bool {
	if a.Clock != b.Clock {
		return a.Clock > b.Clock
	}
	return a.Origin > b.Origin
}

// run 是节点消息处理循环，接收并应用复制消息。
// 接收时更新 Lamport 时钟：max(本地时钟, 消息时钟)+1。
func (n *Node) run(wg *sync.WaitGroup) {
	defer wg.Done()
	for msg := range n.Inbox {
		n.mu.Lock()
		if msg.Value.Clock > n.clock {
			n.clock = msg.Value.Clock
		}
		n.clock++ // receive event
		current, exists := n.store[msg.Key]
		if n.mode == "arrival" || !exists || newer(msg.Value, current) {
			n.store[msg.Key] = msg.Value
		}
		n.mu.Unlock()
		close(msg.Ack)
	}
}

// put 模拟节点本地写入：逻辑时钟递增，生成新版本并立刻写入本地存储。
// 后续复制是异步的，因此其他节点可能暂时读到旧值。
func (n *Node) put(key, data string) Value {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.clock++
	v := Value{Data: data, Clock: n.clock, Origin: n.ID}
	n.store[key] = v
	return v
}

// get 在读锁保护下读取单节点数据，用于检查各副本状态。
func (n *Node) get(key string) (Value, bool) {
	n.mu.RLock()
	defer n.mu.RUnlock()
	v, ok := n.store[key]
	return v, ok
}

// snapshot 复制节点当前数据，避免反熵同步时与并发写入共享可变 map。
func (n *Node) snapshot() map[string]Value {
	n.mu.RLock()
	defer n.mu.RUnlock()
	result := make(map[string]Value, len(n.store))
	for k, v := range n.store {
		result[k] = v
	}
	return result
}

// Network 模拟不可靠网络：随机传输延迟、概率性丢包、异步投递。
// 独立投递的随机延迟可能导致消息乱序，但本程序尚未单独统计乱序次数。
type Network struct {
	nodes              []*Node
	mu                 sync.Mutex
	rng                *rand.Rand
	minDelay, maxDelay time.Duration
	dropRate           float64
	pending            sync.WaitGroup
	sent, dropped      atomic.Int64
}

// newNetwork 使用可配置的随机种子、延迟范围和丢包率。
// 种子固定不代表并发调度完全可复现。
func newNetwork(nodes []*Node, seed int64, minDelay, maxDelay time.Duration, dropRate float64) *Network {
	return &Network{nodes: nodes, rng: rand.New(rand.NewSource(seed)), minDelay: minDelay, maxDelay: maxDelay, dropRate: dropRate}
}

// send 为一条复制消息随机决定是否丢弃、以及延迟多久后投递。
// 每条成功发送的消息对应一个 Goroutine，因延迟不同可能乱序抵达。
func (net *Network) send(from, to int, key string, v Value) {
	net.sent.Add(1)
	net.mu.Lock()
	drop := net.rng.Float64() < net.dropRate
	delay := net.minDelay
	if net.maxDelay > net.minDelay {
		delay += time.Duration(net.rng.Int63n(int64(net.maxDelay-net.minDelay) + 1))
	}
	net.mu.Unlock()
	if drop {
		net.dropped.Add(1)
		return
	}
	net.pending.Add(1)
	go func() {
		defer net.pending.Done()
		time.Sleep(delay)
		ack := make(chan struct{})
		net.nodes[to].Inbox <- Envelope{From: from, To: to, Key: key, Value: v, Ack: ack}
		<-ack // Message has actually been processed, not merely enqueued.
	}()
}

// All calls to wait() happen after producers have finished calling send().
func (net *Network) wait() { net.pending.Wait() }

// broadcast 将当前写入复制到除源节点外的全部节点。
func (net *Network) broadcast(from int, key string, v Value) {
	for to := range net.nodes {
		if to != from {
			net.send(from, to, key, v)
		}
	}
}

// repairRound 执行一轮反熵同步：各节点广播其数据快照，尝试修复丢包导致的副本差异。
// 在 version 模式下，旧版本不会覆盖新版本；在持续丢包时不保证有限轮内收敛。
func (net *Network) repairRound() {
	// Snapshot all nodes before sending, so each round is well-defined.
	for from, node := range net.nodes {
		data := node.snapshot()
		keys := make([]string, 0, len(data))
		for k := range data {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, k := range keys {
			net.broadcast(from, k, data[k])
		}
	}
	net.wait()
}

// Cluster 管理一组节点以及共享的模拟网络，并统一负责关闭资源。
type Cluster struct {
	nodes   []*Node
	net     *Network
	workers sync.WaitGroup
}

// newCluster 为每个节点启动一个独立的消息处理 Goroutine。
func newCluster(count int, mode string, seed int64, minDelay, maxDelay time.Duration, loss float64) *Cluster {
	c := &Cluster{}
	for i := 0; i < count; i++ {
		c.nodes = append(c.nodes, newNode(i+1, mode))
	}
	c.net = newNetwork(c.nodes, seed, minDelay, maxDelay, loss)
	for _, n := range c.nodes {
		c.workers.Add(1)
		go n.run(&c.workers)
	}
	return c
}

// close 先等待网络投递完成，再关闭节点收件箱，防止向已关闭的 channel 发送。
func (c *Cluster) close() {
	c.net.wait()
	for _, n := range c.nodes {
		close(n.Inbox)
	}
	c.workers.Wait()
}

func (c *Cluster) state(key string) string {
	result := ""
	for _, n := range c.nodes {
		v, ok := n.get(key)
		if !ok {
			result += fmt.Sprintf(" N%d=<empty>", n.ID)
		} else {
			result += fmt.Sprintf(" N%d=%s(v%d,n%d)", n.ID, v.Data, v.Clock, v.Origin)
		}
	}
	return result
}

// converged 检查所有节点是否保存了完全相同的 Value。
// 这只是本次实验结束时的副本一致性检查，不等同于线性一致性证明。
func (c *Cluster) converged(key string) bool {
	first, ok := c.nodes[0].get(key)
	if !ok {
		return false
	}
	for _, node := range c.nodes[1:] {
		other, exists := node.get(key)
		if !exists || other != first {
			return false
		}
	}
	return true
}

// Result 保存实验场景、网络参数、是否收敛以及通信开销等指标。
type Result struct {
	Scenario               string
	Mode                   string
	Seed                   int64
	DelayMinMS, DelayMaxMS int64
	DropRate               float64
	Converged              bool
	DurationMS             int64
	Messages, Dropped      int64
	Rounds                 int
}

// experimentDelay 对比异步写入之后、与所有消息处理完成之后的副本状态。
func experimentDelay(seed int64, minD, maxD time.Duration) Result {
	c := newCluster(3, "version", seed, minD, maxD, 0)
	defer c.close()
	key := "x"
	initial := c.nodes[0].put(key, "0")
	c.net.broadcast(0, key, initial)
	c.net.wait()
	start := time.Now()
	update := c.nodes[0].put(key, "1")
	c.net.broadcast(0, key, update)
	fmt.Println("[延迟] 写入后立即:", c.state(key))
	c.net.wait()
	duration := time.Since(start)
	fmt.Println("[延迟] 所有投递处理完成:", c.state(key))
	return Result{"delay", "version", seed, minD.Milliseconds(), maxD.Milliseconds(), 0, c.converged(key), duration.Milliseconds(), c.net.sent.Load(), c.net.dropped.Load(), 0}
}

// experimentConflict 让两个节点并发写相同 key，观察两种冲突解决策略。
// startGate 只协调写入开始，不保证两个 put 在精确的同一时刻执行。
func experimentConflict(seed int64, minD, maxD time.Duration, mode string) Result {
	c := newCluster(3, mode, seed, minD, maxD, 0)
	defer c.close()
	key := "user:1"
	var writers sync.WaitGroup
	startGate := make(chan struct{})
	start := time.Now()
	for i, name := range []string{"Alice", "Bob"} {
		writers.Add(1)
		go func(i int, name string) {
			defer writers.Done()
			<-startGate
			v := c.nodes[i].put(key, name)
			c.net.broadcast(i, key, v)
		}(i, name)
	}
	close(startGate)
	writers.Wait() // No more additions to the network wait group.
	fmt.Printf("[冲突:%s] 写入后: %s\n", mode, c.state(key))
	c.net.wait()
	d := time.Since(start)
	fmt.Printf("[冲突:%s] 投递完成: %s\n", mode, c.state(key))
	return Result{"conflict", mode, seed, minD.Milliseconds(), maxD.Milliseconds(), 0, c.converged(key), d.Milliseconds(), c.net.sent.Load(), c.net.dropped.Load(), 0}
}

// experimentLoss 先模拟有丢包的单次复制，再进行有限轮反熵修复。
// loss_no_repair 和 loss_with_repair 分别记录修复前后的状态。
func experimentLoss(seed int64, minD, maxD time.Duration, loss float64, maxRounds int) (Result, Result) {
	c := newCluster(3, "version", seed, minD, maxD, loss)
	defer c.close()
	key := "x"
	start := time.Now()
	update := c.nodes[0].put(key, "new")
	c.net.broadcast(0, key, update)
	c.net.wait()
	before := Result{"loss_no_repair", "version", seed, minD.Milliseconds(), maxD.Milliseconds(), loss, c.converged(key), time.Since(start).Milliseconds(), c.net.sent.Load(), c.net.dropped.Load(), 0}
	fmt.Println("[丢包] 单次复制后:", c.state(key), "丢弃:", c.net.dropped.Load())
	rounds := 0
	for !c.converged(key) && rounds < maxRounds {
		rounds++
		c.net.repairRound()
	}
	after := Result{"loss_with_repair", "version", seed, minD.Milliseconds(), maxD.Milliseconds(), loss, c.converged(key), time.Since(start).Milliseconds(), c.net.sent.Load(), c.net.dropped.Load(), rounds}
	fmt.Printf("[修复] %d 轮后: %s, 收敛=%v\n", rounds, c.state(key), after.Converged)
	return before, after
}

// writeCSV 将每次实验的观测结果导出，方便后续重复实验和画图分析。
func writeCSV(path string, records []Result) error {
	file, err := os.Create(path)
	if err != nil {
		return err
	}
	defer file.Close()
	w := csv.NewWriter(file)
	if err := w.Write([]string{"scenario", "mode", "seed", "min_delay_ms", "max_delay_ms", "drop_rate", "converged", "elapsed_ms", "messages", "dropped", "repair_rounds"}); err != nil {
		return err
	}
	for _, r := range records {
		if err := w.Write([]string{r.Scenario, r.Mode, strconv.FormatInt(r.Seed, 10), strconv.FormatInt(r.DelayMinMS, 10), strconv.FormatInt(r.DelayMaxMS, 10), strconv.FormatFloat(r.DropRate, 'f', 3, 64), strconv.FormatBool(r.Converged), strconv.FormatInt(r.DurationMS, 10), strconv.FormatInt(r.Messages, 10), strconv.FormatInt(r.Dropped, 10), strconv.Itoa(r.Rounds)}); err != nil {
			return err
		}
	}
	w.Flush()
	return w.Error()
}

// main 解析命令行参数、校验配置、执行多轮实验并写入 CSV。
func main() {
	scenario := flag.String("scenario", "all", "all | delay | conflict | loss")
	seed := flag.Int64("seed", 42, "random seed (scheduling may still vary)")
	minMS := flag.Int("min-delay", 50, "minimum network delay in ms")
	maxMS := flag.Int("max-delay", 200, "maximum network delay in ms")
	loss := flag.Float64("loss", 0.3, "drop rate for loss scenario (0..1)")
	rounds := flag.Int("rounds", 15, "maximum anti-entropy rounds")
	trials := flag.Int("trials", 1, "independent repetitions; vary seed each run")
	output := flag.String("out", "results.csv", "CSV result path")
	flag.Parse()
	if *minMS < 0 || *maxMS < *minMS || *loss < 0 || *loss > 1 || *rounds < 0 || *trials < 1 {
		fmt.Fprintln(os.Stderr, "invalid configuration")
		os.Exit(2)
	}
	if *scenario != "all" && *scenario != "delay" && *scenario != "conflict" && *scenario != "loss" {
		fmt.Fprintln(os.Stderr, "unknown scenario")
		os.Exit(2)
	}
	minD, maxD := time.Duration(*minMS)*time.Millisecond, time.Duration(*maxMS)*time.Millisecond
	records := make([]Result, 0)
	for i := 0; i < *trials; i++ {
		s := *seed + int64(i)
		fmt.Printf("\n===== Trial %d | seed=%d =====\n", i+1, s)
		if *scenario == "all" || *scenario == "delay" {
			records = append(records, experimentDelay(s, minD, maxD))
		}
		if *scenario == "all" || *scenario == "conflict" {
			records = append(records, experimentConflict(s, minD, maxD, "arrival"))
			records = append(records, experimentConflict(s, minD, maxD, "version"))
		}
		if *scenario == "all" || *scenario == "loss" {
			before, after := experimentLoss(s, minD, maxD, *loss, *rounds)
			records = append(records, before, after)
		}
	}
	if err := writeCSV(*output, records); err != nil {
		fmt.Fprintln(os.Stderr, "CSV error:", err)
		os.Exit(1)
	}
	fmt.Printf("\nSaved %d rows to %s\n", len(records), *output)
}
