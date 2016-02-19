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
import "encoding/json"
import "github.com/garyburd/redigo/redis"
import "im_service/common"

var storage *Storage
var config *StorageConfig
var master *Master
var group_manager *GroupManager
var clients ClientSet
var mutex sync.Mutex
var redis_pool *redis.Pool

const GROUP_C_COUNT = 10

var group_c []chan func()

func init() {
	clients = NewClientSet()
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

//clone when write, lockless when read
func AddClient(client *Client) {
	mutex.Lock()
	defer mutex.Unlock()

	if clients.IsMember(client) {
		return
	}
	c := clients.Clone()
	c.Add(client)
	clients = c
}

func RemoveClient(client *Client) {
	mutex.Lock()
	defer mutex.Unlock()

	if !clients.IsMember(client) {
		return
	}
	c := clients.Clone()
	c.Remove(client)
	clients = c
}

//group im
func FindGroupClientSet(appid int64, gid int64) ClientSet {
	s := NewClientSet()

	for c := range clients {
		if c.ContainAppGroupID(appid, gid) {
			s.Add(c)
		}
	}
	return s
}

func IsGroupUserOnline(appid int64, gid int64, uid int64) bool {
	for c := range clients {
		if c.ContainGroupUserID(appid, gid, uid) {
			return true
		}
	}
	return false
}

//peer im
func FindClientSet(id *AppUserID) ClientSet {
	s := NewClientSet()

	for c := range clients {
		if c.ContainAppUserID(id) {
			s.Add(c)
		}
	}
	return s
}

func IsUserOnline(appid int64, uid int64) bool {
	id := &AppUserID{appid: appid, uid: uid}
	for c := range clients {
		if c.ContainAppUserID(id) {
			return true
		}
	}
	return false
}

type Route struct {
	appid  int64
	mutex  sync.Mutex
	groups map[int64]*Group
	uids   common.IntSet
}

func NewRoute(appid int64) *Route {
	r := new(Route)
	r.appid = appid
	r.groups = make(map[int64]*Group)
	r.uids = common.NewIntSet()
	return r
}

func (route *Route) ContainUserID(uid int64) bool {
	route.mutex.Lock()
	defer route.mutex.Unlock()

	return route.uids.IsMember(uid)
}

func (route *Route) AddUserID(uid int64) {
	route.mutex.Lock()
	defer route.mutex.Unlock()

	route.uids.Add(uid)
}

func (route *Route) RemoveUserID(uid int64) {
	route.mutex.Lock()
	defer route.mutex.Unlock()

	route.uids.Remove(uid)
}

func (route *Route) AddGroupMember(gid int64, member int64) {
	route.mutex.Lock()
	defer route.mutex.Unlock()

	if group, ok := route.groups[gid]; ok {
		group.AddMember(member)
	} else {
		members := []int64{member}
		group = NewGroup(gid, route.appid, members)
		route.groups[gid] = group
	}
}

func (route *Route) RemoveGroupMember(gid int64, member int64) {
	route.mutex.Lock()
	defer route.mutex.Unlock()

	if group, ok := route.groups[gid]; ok {
		group.RemoveMember(member)
		if group.IsEmpty() {
			delete(route.groups, gid)
		}
	}
}

func (route *Route) ContainGroupID(gid int64) bool {
	route.mutex.Lock()
	defer route.mutex.Unlock()

	_, ok := route.groups[gid]
	return ok
}

func (route *Route) ContainGroupMember(gid int64, member int64) bool {
	route.mutex.Lock()
	defer route.mutex.Unlock()

	if group, ok := route.groups[gid]; ok {
		return group.IsMember(member)
	}
	return false
}

type Client struct {
	conn *net.TCPConn

	//subscribe mode
	wt        chan *Message
	app_route *AppRoute
}

func NewClient(conn *net.TCPConn) *Client {
	client := new(Client)
	client.conn = conn

	client.wt = make(chan *Message, 10)
	client.app_route = NewAppRoute()
	return client
}

func (client *Client) ContainAppGroupID(appid int64, gid int64) bool {
	route := client.app_route.FindRoute(appid)
	if route == nil {
		return false
	}
	return route.ContainGroupID(gid)
}

func (client *Client) ContainGroupUserID(appid int64, gid int64, uid int64) bool {
	route := client.app_route.FindRoute(appid)
	if route == nil {
		return false
	}

	return route.ContainGroupMember(gid, uid)
}

func (client *Client) ContainAppUserID(id *AppUserID) bool {
	route := client.app_route.FindRoute(id.appid)
	if route == nil {
		return false
	}

	return route.ContainUserID(id.uid)
}

func (client *Client) Read() {
	for {
		msg := client.read()
		if msg == nil {
			RemoveClient(client)
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

//定制推送脚本的app
func (client *Client) IsROMApp(appid int64) bool {
	return false
}

//离线消息入apns队列
func (client *Client) PublishPeerMessage(appid int64, im *IMMessage) {
	conn := redis_pool.Get()
	defer conn.Close()

	v := make(map[string]interface{})
	v["appid"] = appid
	v["sender"] = im.sender
	v["receiver"] = im.receiver
	v["content"] = im.content

	b, _ := json.Marshal(v)
	var queue_name string
	if client.IsROMApp(appid) {
		queue_name = fmt.Sprintf("push_queue_%d", appid)
	} else {
		queue_name = "push_queue"
	}
	_, err := conn.Do("RPUSH", queue_name, b)
	if err != nil {
		log.Info("rpush error:", err)
	}
}

func (client *Client) PublishGroupMessage(appid int64, receivers []int64, im *IMMessage) {
	conn := redis_pool.Get()
	defer conn.Close()

	v := make(map[string]interface{})
	v["appid"] = appid
	v["sender"] = im.sender
	v["receivers"] = receivers
	v["content"] = im.content
	v["group_id"] = im.receiver

	b, _ := json.Marshal(v)
	var queue_name string
	if client.IsROMApp(appid) {
		queue_name = fmt.Sprintf("push_queue_%d", appid)
	} else {
		queue_name = "push_queue"
	}
	_, err := conn.Do("RPUSH", queue_name, b)
	if err != nil {
		log.Info("rpush error:", err)
	}
}

func (client *Client) PublishCSMessage(appid, receiver int64, cs *CustomerServiceMessage) {
	conn := redis_pool.Get()
	defer conn.Close()

	v := make(map[string]interface{})
	v["appid"] = appid
	v["sender"] = cs.sender
	v["receiver"] = receiver
	v["content"] = cs.content

	b, _ := json.Marshal(v)
	var queue_name string
	if client.IsROMApp(appid) {
		queue_name = fmt.Sprintf("push_queue_%d", appid)
	} else {
		queue_name = "push_queue"
	}
	_, err := conn.Do("RPUSH", queue_name, b)
	if err != nil {
		log.Info("rpush error:", err)
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

		s := FindGroupClientSet(appid, gid)
		for c := range s {
			log.Info("publish group message")
			am := &AppMessage{appid: appid, receiver: gid, msgid: msgid, device_id: sae.device_id, msg: sae.msg}
			m := &Message{cmd: MSG_PUBLISH_GROUP, body: am}
			c.wt <- m
		}
		if len(s) == 0 {
			log.Infof("can't publish group message:%d", gid)
		}
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

	group := group_manager.FindGroup(gid)

	if group != nil {
		members := group.Members()
		off_members := make([]int64, 0)

		im := sae.msg.body.(*IMMessage)
		for uid, _ := range members {
			if im.sender != uid && !IsGroupUserOnline(appid, gid, uid) {
				off_members = append(off_members, uid)
			}
		}
		if len(off_members) > 0 {
			client.PublishGroupMessage(appid, off_members, im)
		}
	}
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

		id := &AppUserID{appid: appid, uid: uid}
		s := FindClientSet(id)
		for c := range s {
			am := &AppMessage{appid: appid, receiver: uid, msgid: msgid, device_id: sae.device_id, msg: sae.msg}
			m := &Message{cmd: MSG_PUBLISH, body: am}
			c.wt <- m
		}
		if len(s) == 0 {
			log.Infof("can't publish message:%s %d", Command(sae.msg.cmd), uid)
		}
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

	if sae.msg.cmd == MSG_IM {
		im := sae.msg.body.(*IMMessage)
		if im.receiver == uid && !IsUserOnline(appid, uid) {
			client.PublishPeerMessage(appid, sae.msg.body.(*IMMessage))
		}
	} else if sae.msg.cmd == MSG_GROUP_IM {
		im := sae.msg.body.(*IMMessage)
		if im.sender != uid && !IsUserOnline(appid, uid) {
			client.PublishGroupMessage(appid, []int64{uid}, sae.msg.body.(*IMMessage))
		}
	} else if sae.msg.cmd == MSG_CUSTOMER_SERVICE {
		cs := sae.msg.body.(*CustomerServiceMessage)
		if cs.sender != uid && !IsUserOnline(appid, uid) {
			client.PublishCSMessage(appid, uid, sae.msg.body.(*CustomerServiceMessage))
		}
	}
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

func (client *Client) HandleLoadHistory(lh *LoadHistory) {
	messages := storage.LoadLatestMessages(lh.app_uid.appid, lh.app_uid.uid, int(lh.limit))
	result := &MessageResult{status: 0}
	buffer := new(bytes.Buffer)
	var count int16
	count = int16(len(messages))
	binary.Write(buffer, binary.BigEndian, count)
	for _, emsg := range messages {
		ebuf := client.WriteEMessage(emsg)
		var size int16 = int16(len(ebuf))
		binary.Write(buffer, binary.BigEndian, size)
		buffer.Write(ebuf)
	}
	result.content = buffer.Bytes()
	msg := &Message{cmd: MSG_RESULT, body: result}
	SendMessage(client.conn, msg)
}

const GROUP_OFFLINE_LIMIT = 100

func (client *Client) HandleLoadGroupOffline(lh *LoadGroupOffline) {
	messages := storage.LoadGroupOfflineMessage(lh.appid, lh.gid, lh.uid, lh.device_id, GROUP_OFFLINE_LIMIT)
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

func (client *Client) HandleSubscribeGroup(lo *AppGroupMemberID) {
	log.Infof("subscribe group appid:%d gid:%d uid:%d\n", lo.appid, lo.gid, lo.uid)
	AddClient(client)

	route := client.app_route.FindOrAddRoute(lo.appid)
	route.AddGroupMember(lo.gid, lo.uid)
}

func (client *Client) HandleUnSubscribeGroup(id *AppGroupMemberID) {
	route := client.app_route.FindOrAddRoute(id.appid)
	route.RemoveGroupMember(id.gid, id.uid)
}

func (client *Client) HandleSubscribe(id *AppUserID) {
	log.Infof("subscribe appid:%d uid:%d", id.appid, id.uid)
	AddClient(client)

	route := client.app_route.FindOrAddRoute(id.appid)
	route.AddUserID(id.uid)
}

func (client *Client) HandleUnsubscribe(id *AppUserID) {
	log.Infof("unsubscribe appid:%d uid:%d", id.appid, id.uid)
	route := client.app_route.FindOrAddRoute(id.appid)
	route.RemoveUserID(id.uid)
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
	case MSG_LOAD_HISTORY:
		client.HandleLoadHistory((*LoadHistory)(msg.body.(*LoadHistory)))
	case MSG_SAVE_AND_ENQUEUE_GROUP:
		client.HandleSaveAndEnqueueGroup(msg.body.(*SAEMessage))
	case MSG_DEQUEUE_GROUP:
		client.HandleDQGroupMessage(msg.body.(*DQGroupMessage))
	case MSG_SUBSCRIBE_GROUP:
		client.HandleSubscribeGroup(msg.body.(*AppGroupMemberID))
	case MSG_UNSUBSCRIBE_GROUP:
		client.HandleUnSubscribeGroup(msg.body.(*AppGroupMemberID))
	case MSG_LOAD_GROUP_OFFLINE:
		client.HandleLoadGroupOffline(msg.body.(*LoadGroupOffline))
	case MSG_SUBSCRIBE:
		client.HandleSubscribe(msg.body.(*AppUserID))
	case MSG_UNSUBSCRIBE:
		client.HandleUnsubscribe(msg.body.(*AppUserID))
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

func handle_sync_client(conn *net.TCPConn) {
	conn.SetKeepAlive(true)
	conn.SetKeepAlivePeriod(time.Duration(10 * 60 * time.Second))
	client := NewSyncClient(conn)
	client.Run()
}

func ListenSyncClient() {
	Listen(handle_sync_client, config.sync_listen)
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
			storage.FlushReceived()
			os.Exit(0)
		}
	}
	return nil // It'll never get here.
}

func FlushLoop() {
	for {
		time.Sleep(1 * time.Second)
		storage.FlushReceived()
	}
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
	log.Infof("listen:%s storage root:%s sync listen:%s master address:%s\n",
		config.listen, config.storage_root, config.sync_listen, config.master_address)
	//redis连接池
	redis_pool = NewRedisPool(config.redis_address, "")
	//新建/读取消息
	storage = NewStorage(config.storage_root)

	//主机备份机相关
	master = NewMaster()
	master.Start()
	if len(config.master_address) > 0 {
		slaver := NewSlaver(config.master_address)
		slaver.Start()
	}
	//群组初始化
	group_manager = NewGroupManager()
	group_manager.Start()

	for i := 0; i < GROUP_C_COUNT; i++ {
		go GroupLoop(group_c[i])
	}

	//刷新storage缓存的ack
	go FlushLoop()
	go waitSignal()
	//备份监听
	go ListenSyncClient()
	//主机监听
	ListenClient()
}
