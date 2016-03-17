/**
 * Copyright (c) 2014-2015, GoBelieve
 * All rights reserved.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation; either version 2 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
 */

package main

import "net"
import "fmt"
import "bytes"
import "time"
import "sync"
import "runtime"
import "flag"
import "encoding/binary"
import log "github.com/golang/glog"
import "os"
import "os/signal"
import "syscall"
import "github.com/garyburd/redigo/redis"
import "github.com/zheng-ji/goSnowFlake"

var storage *Storage
var config *StorageConfig
var mutex sync.Mutex
var redis_pool *redis.Pool
var iw *goSnowFlake.IdWorker

const GROUP_C_COUNT = 10

var group_c []chan func()

func init() {
	group_c = make([]chan func(), GROUP_C_COUNT)
	for i := 0; i < GROUP_C_COUNT; i++ {
		group_c[i] = make(chan func())
	}
}

func GetGroupChan(gid int64) chan func() {
	index := gid % GROUP_C_COUNT
	return group_c[index]
}

func GetUserChan(uid int64) chan func() {
	index := uid % GROUP_C_COUNT
	return group_c[index]
}

type Client struct {
	conn *net.TCPConn

	//subscribe mode
	wt        chan *Message
}

func NewClient(conn *net.TCPConn) *Client {
	client := new(Client)
	client.conn = conn

	client.wt = make(chan *Message, 10)
	return client
}

func (client *Client) Read() {
	for {
		msg := client.read()
		if msg == nil {
			client.wt <- nil
			break
		}
		client.HandleMessage(msg)
	}
}

func (client *Client) Write() {
	for {
		msg := <-client.wt
		if msg == nil {
			client.conn.Close()
			break
		}
		SendMessage(client.conn, msg)
	}
}

func (client *Client) HandleSaveAndEnqueueGroup(sae *SAEMessage) {
	if sae.msg == nil {
		log.Error("sae msg is nil")
		return
	}
	if sae.msg.cmd != MSG_GROUP_IM {
		log.Error("sae msg cmd:", sae.msg.cmd)
		return
	}

	appid := sae.appid
	gid := sae.receiver

	//保证群组消息以id递增的顺序发出去
	t := make(chan int64)
	f := func() {
		msgid := storage.SaveGroupMessage(appid, gid, sae.device_id, sae.msg)

		am := &AppMessage{appid: appid, receiver: gid, msgid: msgid, device_id: sae.device_id, msg: sae.msg}
		m := &Message{cmd: MSG_PUBLISH_GROUP, body: am}
		client.wt <- m
			
		t <- msgid
	}

	c := GetGroupChan(gid)
	c <- f
	msgid := <-t

	result := &MessageResult{}
	result.status = 0
	buffer := new(bytes.Buffer)
	binary.Write(buffer, binary.BigEndian, msgid)
	result.content = buffer.Bytes()
	msg := &Message{cmd: MSG_RESULT, body: result}
	SendMessage(client.conn, msg)
}

func (client *Client) HandleDQGroupMessage(dq *DQGroupMessage) {
	if dq.device_id > 0 {
		storage.DequeueGroupOffline(dq.msgid, dq.appid, dq.gid, dq.receiver, dq.device_id)
	}
	result := &MessageResult{status: 0}
	msg := &Message{cmd: MSG_RESULT, body: result}
	SendMessage(client.conn, msg)
}

func (client *Client) HandleSaveAndEnqueue(sae *SAEMessage) {
	if sae.msg == nil {
		log.Error("sae msg is nil")
		return
	}

	appid := sae.appid
	uid := sae.receiver
	//保证消息以id递增的顺序发出
	t := make(chan int64)
	f := func() {
		msgid := storage.SavePeerMessage(appid, uid, sae.device_id, sae.msg)

		am := &AppMessage{appid: appid, receiver: uid, msgid: msgid, device_id: sae.device_id, msg: sae.msg}
		m := &Message{cmd: MSG_PUBLISH, body: am}
		client.wt <- m
		
		t <- msgid
	}

	c := GetUserChan(uid)
	c <- f
	msgid := <-t

	result := &MessageResult{}
	result.status = 0
	buffer := new(bytes.Buffer)
	binary.Write(buffer, binary.BigEndian, msgid)
	result.content = buffer.Bytes()
	msg := &Message{cmd: MSG_RESULT, body: result}
	SendMessage(client.conn, msg)
}

func (client *Client) HandleDQMessage(dq *DQMessage) {
	if dq.device_id != 0 {
		storage.DequeueOffline(dq.msgid, dq.appid, dq.receiver, dq.device_id)
	}
	result := &MessageResult{status: 0}
	msg := &Message{cmd: MSG_RESULT, body: result}
	SendMessage(client.conn, msg)
}

func (client *Client) WriteEMessage(emsg *EMessage) []byte {
	buffer := new(bytes.Buffer)
	binary.Write(buffer, binary.BigEndian, emsg.msgid)
	binary.Write(buffer, binary.BigEndian, emsg.device_id)
	SendMessage(buffer, emsg.msg)
	return buffer.Bytes()
}

func (client *Client) HandleLoadOffline(id *LoadOffline) {
	messages := storage.LoadOfflineMessage(id.appid, id.uid, id.device_id)
	result := &MessageResult{status: 0}
	buffer := new(bytes.Buffer)

	var count int16 = 0
	for _, emsg := range messages {
		if emsg.msg.cmd == MSG_IM ||
			emsg.msg.cmd == MSG_GROUP_IM {
			m := emsg.msg.body.(*IMMessage)
			//同一台设备自己发出的消息
			if m.sender == id.uid && emsg.device_id == id.device_id {
				continue
			}
		}

		if emsg.msg.cmd == MSG_CUSTOMER_SERVICE {
			m := emsg.msg.body.(*CustomerServiceMessage)
			//同一台设备自己发出的消息
			if m.sender == id.uid && emsg.device_id == id.device_id {
				continue
			}
		}

		count += 1
	}

	binary.Write(buffer, binary.BigEndian, count)
	for _, emsg := range messages {
		if emsg.msg.cmd == MSG_IM ||
			emsg.msg.cmd == MSG_GROUP_IM {
			m := emsg.msg.body.(*IMMessage)
			//同一台设备自己发出的消息
		
			if m.sender == id.uid && emsg.device_id == id.device_id {
				continue
			}
		}

		if emsg.msg.cmd == MSG_CUSTOMER_SERVICE {
			m := emsg.msg.body.(*CustomerServiceMessage)
			//同一台设备自己发出的消息
			if m.sender == id.uid && emsg.device_id == id.device_id {
				continue
			}
		}

		ebuf := client.WriteEMessage(emsg)
		var size int16 = int16(len(ebuf))
		binary.Write(buffer, binary.BigEndian, size)
		buffer.Write(ebuf)
	}
	result.content = buffer.Bytes()
	msg := &Message{cmd: MSG_RESULT, body: result}
	SendMessage(client.conn, msg)
}

func (client *Client) HandleLoadGroupOffline(lh *LoadGroupOffline) {
	messages := storage.LoadGroupOfflineMessage(lh.appid, lh.gid, lh.uid, lh.device_id)
	result := &MessageResult{status: 0}
	buffer := new(bytes.Buffer)

	var count int16 = 0
	for _, emsg := range messages {
		if emsg.msg.cmd == MSG_GROUP_IM {
			im := emsg.msg.body.(*IMMessage)
			if im.sender == lh.uid && emsg.device_id == lh.device_id {
				continue
			}
		}
		count += 1
	}
	binary.Write(buffer, binary.BigEndian, count)
	for _, emsg := range messages {
		if emsg.msg.cmd == MSG_GROUP_IM {
			im := emsg.msg.body.(*IMMessage)
			if im.sender == lh.uid && emsg.device_id == lh.device_id {
				continue
			}
		}
		ebuf := client.WriteEMessage(emsg)
		var size int16 = int16(len(ebuf))
		binary.Write(buffer, binary.BigEndian, size)
		buffer.Write(ebuf)
	}
	result.content = buffer.Bytes()
	msg := &Message{cmd: MSG_RESULT, body: result}
	SendMessage(client.conn, msg)
}

//指令处理
func (client *Client) HandleMessage(msg *Message) {
	log.Info("msg cmd:", Command(msg.cmd))
	switch msg.cmd {
	case MSG_LOAD_OFFLINE:
		client.HandleLoadOffline(msg.body.(*LoadOffline))
	case MSG_SAVE_AND_ENQUEUE:
		client.HandleSaveAndEnqueue(msg.body.(*SAEMessage))
	case MSG_DEQUEUE:
		client.HandleDQMessage(msg.body.(*DQMessage))
	case MSG_SAVE_AND_ENQUEUE_GROUP:
		client.HandleSaveAndEnqueueGroup(msg.body.(*SAEMessage))
	case MSG_DEQUEUE_GROUP:
		client.HandleDQGroupMessage(msg.body.(*DQGroupMessage))
	case MSG_LOAD_GROUP_OFFLINE:
		client.HandleLoadGroupOffline(msg.body.(*LoadGroupOffline))
	default:
		log.Warning("unknown msg:", msg.cmd)
	}
}

func (client *Client) Run() {
	go client.Read()
	go client.Write()
}

func (client *Client) read() *Message {
	return ReceiveMessage(client.conn)
}

func (client *Client) send(msg *Message) {
	SendMessage(client.conn, msg)
}

func (client *Client) close() {
	client.conn.Close()
}

func handle_client(conn *net.TCPConn) {
	conn.SetKeepAlive(true)
	conn.SetKeepAlivePeriod(time.Duration(10 * 60 * time.Second))
	client := NewClient(conn)
	client.Run()
}

func Listen(f func(*net.TCPConn), listen_addr string) {
	listen, err := net.Listen("tcp", listen_addr)
	if err != nil {
		fmt.Println("初始化失败", err.Error())
		return
	}
	tcp_listener, ok := listen.(*net.TCPListener)
	if !ok {
		fmt.Println("listen error")
		return
	}

	for {
		client, err := tcp_listener.AcceptTCP()
		if err != nil {
			return
		}
		f(client)
	}
}

func ListenClient() {
	Listen(handle_client, config.listen)
}

func GroupLoop(c chan func()) {
	for {
		f := <-c
		f()
	}
}

// Signal handler
func waitSignal() error {
	ch := make(chan os.Signal, 1)
	signal.Notify(
		ch,
		syscall.SIGINT,
		syscall.SIGTERM,
	)
	for {
		sig := <-ch
		fmt.Println("singal:", sig.String())
		switch sig {
		case syscall.SIGTERM, syscall.SIGINT:
			os.Exit(0)
		}
	}
	return nil // It'll never get here.
}

//redis连接池
func NewRedisPool(server, password string) *redis.Pool {
	return &redis.Pool{
		MaxIdle:     100,
		MaxActive:   500,
		IdleTimeout: 480 * time.Second,
		Dial: func() (redis.Conn, error) {
			c, err := redis.Dial("tcp", server)
			if err != nil {
				return nil, err
			}
			if len(password) > 0 {
				if _, err := c.Do("AUTH", password); err != nil {
					c.Close()
					return nil, err
				}
			}
			return c, err
		},
	}
}

func main() {
	//cpu
	runtime.GOMAXPROCS(runtime.NumCPU())
	//读参数
	flag.Parse()
	//参数为im.cfg文件地址
	if len(flag.Args()) == 0 {
		fmt.Println("usage: im config")
		return
	}
	//读取配置
	config = read_storage_cfg(flag.Args()[0])
	log.Infof("listen:%s\n", config.listen)
	//redis连接池
	redis_pool = NewRedisPool(config.redis_address, config.redis_password)
	for i := 0; i < GROUP_C_COUNT; i++ {
		go GroupLoop(group_c[i])
	}
	
	//msg id生产器
	niw, err := goSnowFlake.NewIdWorker(1)
	if err != nil {
		fmt.Println(err)
		return
	}
	iw = niw
	//新建/读取消息
	storage = NewStorage(config.ots_endpoint, config.ots_accessid, config.ots_accesskey, config.ots_instancename)

	go waitSignal()

	//主机监听
	ListenClient()
}