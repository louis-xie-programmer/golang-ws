// main.go
package main

import (
	"bytes"
	"container/heap"
	"context"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/fasthttp/websocket"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/valyala/fasthttp"
	"github.com/valyala/fasthttp/fasthttpadaptor"
)

// ===========================
// 配置
// ===========================
const (
	addr = ":8080"

	maxMessageSize = 1 << 20 // 1MB
	writeWait      = 10 * time.Second
	pongWait       = 60 * time.Second
	pingPeriod     = (pongWait * 9) / 10

	sendQueueSize = 256       // 每个连接优先级队列最大长度
	maxBatchSize  = 64 * 1024 // 批量写最大合并字节数
	initialBufCap = 4 * 1024

	workerPoolSize      = 8   // worker goroutines 数量（可根据机器调整）
	workerQueueCapacity = 1024 // worker 待处理任务队列长度
)

// ===========================
// Prometheus 指标
// ===========================
var (
	activeConnections = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "ws_active_connections",
		Help: "当前活跃连接数",
	})

	messagesSent = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "ws_messages_sent_total",
		Help: "发送消息总数",
	})

	messagesDropped = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "ws_messages_dropped_total",
		Help: "丢弃发送消息总数（优先级队列满）",
	})

	sendQueueLength = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "ws_send_queue_length",
		Help: "所有连接发送队列长度总和（示例汇总）",
	})

	tasksSubmitted = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "ws_tasks_submitted_total",
		Help: "提交给 worker pool 的任务总数",
	})

	tasksDropped = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "ws_tasks_dropped_total",
		Help: "提交给 worker pool 时被丢弃的任务总数（队列满）",
	})

	workerQueueLen = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "ws_worker_queue_length",
		Help: "worker pool 当前任务队列长度",
	})
)

func init() {
	promHandler = fasthttpadaptor.NewFastHTTPHandler(promhttp.Handler())
	prometheus.MustRegister(activeConnections, messagesSent, messagesDropped, sendQueueLength, tasksSubmitted, tasksDropped, workerQueueLen)
}

// ===========================
// buffer 池（用于复用 byte slices）
// ===========================
var bufPool = sync.Pool{
	New: func() interface{} {
		b := make([]byte, 0, initialBufCap)
		return &b
	},
}

// ===========================
// 优先级队列（小顶堆）
// ===========================
type priorityMsg struct {
	data     []byte
	priority int // 数字越小优先级越高
	index    int
}

type PriorityQueue []*priorityMsg

func (pq PriorityQueue) Len() int { return len(pq) }
func (pq PriorityQueue) Less(i, j int) bool {
	return pq[i].priority < pq[j].priority
}
func (pq PriorityQueue) Swap(i, j int) {
	pq[i], pq[j] = pq[j], pq[i]
	pq[i].index = i
	pq[j].index = j
}
func (pq *PriorityQueue) Push(x interface{}) {
	item := x.(*priorityMsg)
	item.index = len(*pq)
	*pq = append(*pq, item)
}
func (pq *PriorityQueue) Pop() interface{} {
	old := *pq
	n := len(old)
	item := old[n-1]
	item.index = -1
	*pq = old[0 : n-1]
	return item
}

// ===========================
// Hub / Room 管理
// ===========================
type Room struct {
	name  string
	conns map[*ConnWrap]bool
	lock  sync.RWMutex
}

func NewRoom(name string) *Room {
	return &Room{name: name, conns: make(map[*ConnWrap]bool)}
}

func (r *Room) AddConn(c *ConnWrap) {
	r.lock.Lock()
	defer r.lock.Unlock()
	r.conns[c] = true
	c.room = r
}

func (r *Room) RemoveConn(c *ConnWrap) {
	r.lock.Lock()
	defer r.lock.Unlock()
	delete(r.conns, c)
}

func (r *Room) Broadcast(msg []byte, priority int) {
	r.lock.RLock()
	defer r.lock.RUnlock()
	for c := range r.conns {
		_ = c.Send(msg, priority) // 忽略返回值（可记录失败）
	}
}

type Hub struct {
	rooms map[string]*Room
	lock  sync.RWMutex
}

var hub = &Hub{rooms: make(map[string]*Room)}

func (h *Hub) GetOrCreateRoom(name string) *Room {
	h.lock.Lock()
	defer h.lock.Unlock()
	if r, ok := h.rooms[name]; ok {
		return r
	}
	r := NewRoom(name)
	h.rooms[name] = r
	return r
}

func (h *Hub) JoinRoom(name string, c *ConnWrap) {
	r := h.GetOrCreateRoom(name)
	r.AddConn(c)
}

func (h *Hub) BroadcastRoom(name string, msg []byte, priority int) {
	h.lock.RLock()
	r, ok := h.rooms[name]
	h.lock.RUnlock()
	if !ok {
		return
	}
	r.Broadcast(msg, priority)
}

func (h *Hub) BroadcastAll(msg []byte, priority int) {
	h.lock.RLock()
	defer h.lock.RUnlock()
	for _, r := range h.rooms {
		r.Broadcast(msg, priority)
	}
}

// ===========================
// ConnWrap：连接封装（优先级队列 + 读写 pump）
// ===========================
type ConnWrap struct {
	conn   *websocket.Conn
	ctx    context.Context
	cancel context.CancelFunc

	room *Room

	pq     PriorityQueue
	pqCond *sync.Cond

	closed int32
	wg     sync.WaitGroup
}

func NewConnWrap(ws *websocket.Conn) *ConnWrap {
	ctx, cancel := context.WithCancel(context.Background())
	c := &ConnWrap{
		conn:   ws,
		ctx:    ctx,
		cancel: cancel,
		pqCond: sync.NewCond(&sync.Mutex{}),
	}
	heap.Init(&c.pq)
	activeConnections.Inc()

	c.wg.Add(2)
	go c.readPump()
	go c.writePump()
	return c
}

func (c *ConnWrap) Close() {
	if !atomic.CompareAndSwapInt32(&c.closed, 0, 1) {
		return
	}
	c.cancel()
	_ = c.conn.Close()
	c.wg.Wait()
	activeConnections.Dec()
	if c.room != nil {
		c.room.RemoveConn(c)
	}
}

func (c *ConnWrap) Send(data []byte, priority int) bool {
	if atomic.LoadInt32(&c.closed) == 1 {
		return false
	}
	p := bufPool.Get().(*[]byte)
	*p = (*p)[:0]
	*p = append(*p, data...)

	item := &priorityMsg{data: *p, priority: priority}

	c.pqCond.L.Lock()
	if len(c.pq) >= sendQueueSize {
		// 队列已满：丢弃策略
		c.pqCond.L.Unlock()
		bufPool.Put(p)
		messagesDropped.Inc()
		sendQueueLength.Set(float64(len(c.pq)))
		return false
	}
	heap.Push(&c.pq, item)
	c.pqCond.Signal()
	c.pqCond.L.Unlock()

	sendQueueLength.Set(float64(len(c.pq)))
	return true
}

func (c *ConnWrap) readPump() {
	defer func() {
		c.cancel()
		c.wg.Done()
	}()

	c.conn.SetReadLimit(maxMessageSize)
	c.conn.SetReadDeadline(time.Now().Add(pongWait))
	c.conn.SetPongHandler(func(string) error {
		c.conn.SetReadDeadline(time.Now().Add(pongWait))
		return nil
	})

	for {
		_, msg, err := c.conn.ReadMessage()
		if err != nil {
			// 读取错误或连接断开
			return
		}

		// 这里假设客户端发来的业务消息具有简单前缀逻辑：
		// "join:roomA" -> 加入房间
		// "task:...": 作为业务任务提交给 worker pool
		// "msg:..." -> 业务直接广播（异步也可以）
		if bytes.HasPrefix(msg, []byte("join:")) {
			roomName := string(msg[5:])
			hub.JoinRoom(roomName, c)
			// 回写确认（低优先级）
			_ = c.Send([]byte("joined:"+roomName), 50)
			continue
		}
		if bytes.HasPrefix(msg, []byte("msg:")) {
			// 直接广播到当前房间（优先级示例：10）
			if c.room != nil {
				hub.BroadcastRoom(c.room.name, msg[4:], 10)
			}
			continue
		}
		if bytes.HasPrefix(msg, []byte("task:")) {
			// 把任务提交给 worker pool（异步处理）
			taskData := append([]byte{}, msg[5:]...) // 复制（或直接传 msg 视安全性而定）
			ok := workerPool.Submit(Task{
				Conn: c,
				Data: taskData,
			})
			if !ok {
				tasksDropped.Inc()
				// 通知客户端任务被拒绝（可选）
				_ = c.Send([]byte("task_rejected"), 1)
			} else {
				tasksSubmitted.Inc()
			}
			continue
		}
		// 其他类型：回写 echo（低优先级）
		_ = c.Send(append([]byte("echo:"), msg...), 100)
	}
}

func (c *ConnWrap) writePump() {
	defer func() {
		// 清空队列并回收 slice
		c.pqCond.L.Lock()
		for len(c.pq) > 0 {
			item := heap.Pop(&c.pq).(*priorityMsg)
			bufPool.Put(&item.data)
		}
		c.pqCond.L.Unlock()
		c.wg.Done()
	}()

	ticker := time.NewTicker(pingPeriod)
	defer ticker.Stop()

	for {
		select {
		case <-c.ctx.Done():
			return
		case <-ticker.C:
			_ = c.conn.SetWriteDeadline(time.Now().Add(writeWait))
			_ = c.conn.WriteControl(websocket.PingMessage, []byte{}, time.Now().Add(writeWait))
		default:
			c.pqCond.L.Lock()
			for len(c.pq) == 0 {
				c.pqCond.Wait()
				if c.ctx.Err() != nil {
					c.pqCond.L.Unlock()
					return
				}
			}
			// 批量取出若干消息
			var batch [][]byte
			total := 0
			for len(c.pq) > 0 && total < maxBatchSize {
				item := heap.Pop(&c.pq).(*priorityMsg)
				batch = append(batch, item.data)
				total += len(item.data)
			}
			c.pqCond.L.Unlock()

			_ = c.conn.SetWriteDeadline(time.Now().Add(writeWait))
			if len(batch) == 1 {
				if err := c.conn.WriteMessage(websocket.TextMessage, batch[0]); err != nil {
					return
				}
			} else {
				var buf bytes.Buffer
				for _, b := range batch {
					// 若需要边界，可在这里加入长度前缀；示例直接拼接
					buf.Write(b)
				}
				if err := c.conn.WriteMessage(websocket.TextMessage, buf.Bytes()); err != nil {
					return
				}
			}

			for _, b := range batch {
				bufPool.Put(&b)
			}
			messagesSent.Add(float64(len(batch)))
		}
	}
}

// ===========================
// Worker Pool（处理业务任务）
// ===========================
type Task struct {
	Conn *ConnWrap
	Data []byte
}

type WorkerPool struct {
	tasks chan Task
	wg    sync.WaitGroup
	quit  chan struct{}
}

var workerPool *WorkerPool

func NewWorkerPool(n int, cap int) *WorkerPool {
	wp := &WorkerPool{
		tasks: make(chan Task, cap),
		quit:  make(chan struct{}),
	}
	wp.wg.Add(n)
	for i := 0; i < n; i++ {
		go func(id int) {
			defer wp.wg.Done()
			for {
				select {
				case t, ok := <-wp.tasks:
					if !ok {
						return
					}
					// 处理任务（示例业务逻辑）
					handleTask(id, t)
					workerQueueLen.Set(float64(len(wp.tasks)))
				case <-wp.quit:
					return
				}
			}
		}(i)
	}
	return wp
}

// Submit 尝试提交任务到池，非阻塞；队列满则返回 false
func (wp *WorkerPool) Submit(t Task) bool {
	select {
	case wp.tasks <- t:
		workerQueueLen.Set(float64(len(wp.tasks)))
		return true
	default:
		return false
	}
}

func (wp *WorkerPool) Shutdown() {
	close(wp.tasks)
	close(wp.quit)
	wp.wg.Wait()
}

// 示例业务处理函数：可以替换为实际逻辑（DB、网络调用等）
func handleTask(workerID int, t Task) {
	// 模拟处理耗时（真实场景请去掉或替换）
	// time.Sleep(10 * time.Millisecond)

	// 处理结果：给原连接回写或广播到房间
	result := []byte(fmt.Sprintf("processed_by_%d:%s", workerID, string(t.Data)))

	// 如果该连接仍活着，则发送回写（高优先级）
	if atomic.LoadInt32(&t.Conn.closed) == 0 {
		_ = t.Conn.Send(result, 5) // 优先级 5（较高）
	}
	// 此外也可以选择广播到房间（示例）
	if t.Conn.room != nil {
		t.Conn.room.Broadcast([]byte("room_broadcast:"+string(t.Data)), 20)
	}
}

// ===========================
// fasthttp + websocket handler
// ===========================
var upgrader = websocket.FastHTTPUpgrader{
	CheckOrigin: func(ctx *fasthttp.RequestCtx) bool { return true },
}

func wsHandler(ctx *fasthttp.RequestCtx) {
	err := upgrader.Upgrade(ctx, func(ws *websocket.Conn) {
		c := NewConnWrap(ws)
		// 等待连接关闭
		<-c.ctx.Done()
		c.Close()
	})
	if err != nil {
		log.Printf("upgrade error: %v", err)
	}
}

// 将 promhttp.Handler 转为 fasthttp Handler（使用 fasthttpadaptor）
var promHandler fasthttp.RequestHandler

// func init() {
// 	promHandler = fasthttpadaptor.NewFastHTTPHandler(promhttp.Handler())
// }

// ===========================
// main：启动 server、worker pool、优雅关机
// ===========================
func main() {
	// 启动 worker pool
	workerPool = NewWorkerPool(workerPoolSize, workerQueueCapacity)
	log.Printf("worker pool started: size=%d, queue=%d", workerPoolSize, workerQueueCapacity)

	srv := &fasthttp.Server{
		Handler: func(ctx *fasthttp.RequestCtx) {
			switch string(ctx.Path()) {
			case "/ws":
				wsHandler(ctx)
			case "/metrics":
				promHandler(ctx)
			default:
				ctx.SetStatusCode(fasthttp.StatusNotFound)
			}
		},
	}

	ln, err := net.Listen("tcp4", addr)
	if err != nil {
		log.Fatalf("listen: %v", err)
	}

	go func() {
		log.Printf("listening on %s", addr)
		if err := srv.Serve(ln); err != nil {
			log.Fatalf("server error: %v", err)
		}
	}()

	// 优雅关闭
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh
	log.Println("shutdown: stopping server and worker pool...")

	_ = srv.Shutdown()
	workerPool.Shutdown()
	log.Println("shutdown complete")
}
