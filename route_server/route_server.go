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
import "sync"
import "runtime"
import "flag"
import "fmt"
import "time"
import "bytes"
import "encoding/binary"
import "encoding/json"
import log "github.com/golang/glog"
import "github.com/garyburd/redigo/redis"
import "im_service/common"

var config *RouteConfig
var clients ClientSet
var mutex   sync.Mutex
var redis_pool *redis.Pool

func init() {
	clients = NewClientSet()
}

func AddClient(client *Client) {
	mutex.Lock()
	defer mutex.Unlock()
	
	clients.Add(client)
}

func RemoveClient(client *Client) {
	mutex.Lock()
	defer mutex.Unlock()

	clients.Remove(client)
}

func FindClientSet(id *AppUserID) ClientSet {
	mutex.Lock()
	defer mutex.Unlock()

	s := NewClientSet()

	for c := range(clients) {
		if c.ContainAppUserID(id) {
			s.Add(c)
		}
	}
	return s
}


func FindRoomClientSet(id *AppRoomID) ClientSet {
	mutex.Lock()
	defer mutex.Unlock()

	s := NewClientSet()

	for c := range(clients) {
		if c.ContainAppRoomID(id) {
			s.Add(c)
		}
	}
	return s
}

type Route struct {
	appid     int64
	mutex     sync.Mutex
	uids      common.IntSet
	room_ids  common.IntSet
}

func NewRoute(appid int64) *Route {
	r := new(Route)
	r.appid = appid
	r.uids = common.NewIntSet()
	r.room_ids = common.NewIntSet()
	return r
}


func (route *Route) IsIntersect(s common.IntSet) bool {
	route.mutex.Lock()
	defer route.mutex.Unlock()
	
	for uid := range(route.uids) {
		if s.IsMember(uid) {
			return true
		}
	}
	return false
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

func (route *Route) ContainRoomID(room_id int64) bool {
	route.mutex.Lock()
	defer route.mutex.Unlock()
	
	return route.room_ids.IsMember(room_id)
}

func (route *Route) AddRoomID(room_id int64) {
	route.mutex.Lock()
	defer route.mutex.Unlock()

	route.room_ids.Add(room_id)
}

func (route *Route) RemoveRoomID(room_id int64) {
	route.mutex.Lock()
	defer route.mutex.Unlock()

	route.room_ids.Remove(room_id)
}


type Client struct {
	wt     chan *Message
	
	conn   *net.TCPConn
	app_route *AppRoute
}

func NewClient(conn *net.TCPConn) *Client {
	client := new(Client)
	client.conn = conn 
	client.wt = make(chan *Message, 10)
	client.app_route = NewAppRoute()
	return client
}

func (client *Client) ContainAppUserID(id *AppUserID) bool {
	route := client.app_route.FindRoute(id.appid)
	if route == nil {
		return false
	}

	return route.ContainUserID(id.uid)
}


func (client *Client) ContainAppRoomID(id *AppRoomID) bool {
	route := client.app_route.FindRoute(id.appid)
	if route == nil {
		return false
	}

	return route.ContainRoomID(id.room_id)
}

func (client *Client) Read() {
	AddClient(client)
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

func (client *Client) HandleMessage(msg *Message) {
	log.Info("msg cmd:", Command(msg.cmd))
	switch msg.cmd {
	case MSG_SUBSCRIBE:
		client.HandleSubscribe(msg.body.(*AppUserID))
	case MSG_UNSUBSCRIBE:
		client.HandleUnsubscribe(msg.body.(*AppUserID))
	case MSG_PUBLISH:
		client.HandlePublish(msg.body.(*AppMessage))
	case MSG_SUBSCRIBE_ROOM:
		client.HandleSubscribeRoom(msg.body.(*AppRoomID))
	case MSG_UNSUBSCRIBE_ROOM:
		client.HandleUnsubscribeRoom(msg.body.(*AppRoomID))
	case MSG_PUBLISH_ROOM:
		client.HandlePublishRoom(msg.body.(*AppMessage))
	default:
		log.Warning("unknown message cmd:", msg.cmd)
	}
}

func (client *Client) HandleSubscribe(id *AppUserID) {
	log.Infof("subscribe appid:%d uid:%d", id.appid, id.uid)
	route := client.app_route.FindOrAddRoute(id.appid)
	route.AddUserID(id.uid)
}

func (client *Client) HandleUnsubscribe(id *AppUserID) {
	log.Infof("unsubscribe appid:%d uid:%d", id.appid, id.uid)
	route := client.app_route.FindOrAddRoute(id.appid)
	route.RemoveUserID(id.uid)
}


const VOIP_COMMAND_DIAL = 1
const VOIP_COMMAND_DIAL_VIDEO = 9


func (client *Client) GetDialCount(ctl *VOIPControl) int {
	if len(ctl.content) < 4 {
		return 0
	}

	var ctl_cmd int32
	buffer := bytes.NewBuffer(ctl.content)
	binary.Read(buffer, binary.BigEndian, &ctl_cmd)
	if ctl_cmd != VOIP_COMMAND_DIAL && ctl_cmd != VOIP_COMMAND_DIAL_VIDEO {
		return 0
	}

	if len(ctl.content) < 8 {
		return 0
	}
	var dial_count int32
	binary.Read(buffer, binary.BigEndian, &dial_count)

	return int(dial_count)
}


func (client *Client) IsROMApp(appid int64) bool {
	return false
}

func (client *Client) PublishMessage(appid int64, ctl *VOIPControl) {
	//首次拨号时发送apns通知
	count := client.GetDialCount(ctl)
	if count != 1 {
		return
	}

	log.Infof("publish invite notification sender:%d receiver:%d", ctl.sender, ctl.receiver)
	conn := redis_pool.Get()
	defer conn.Close()

	v := make(map[string]interface{})
	v["sender"] = ctl.sender
	v["receiver"] = ctl.receiver
	v["appid"] = appid
	b, _ := json.Marshal(v)

	var queue_name string
	if client.IsROMApp(appid) {
		queue_name = fmt.Sprintf("voip_push_queue_%d", appid)
	} else {
		queue_name = "voip_push_queue"
	}

	_, err := conn.Do("RPUSH", queue_name, b)
	if err != nil {
		log.Info("error:", err)
	}
}


func (client *Client) HandlePublish(amsg *AppMessage) {
	log.Infof("publish message appid:%d uid:%d msgid:%d cmd:%s", amsg.appid, amsg.receiver, amsg.msgid, Command(amsg.msg.cmd))
	receiver := &AppUserID{appid:amsg.appid, uid:amsg.receiver}
	s := FindClientSet(receiver)

	if len(s) == 0 {
		//用户不在线,推送消息到终端
		if amsg.msg.cmd == MSG_VOIP_CONTROL {
			ctrl := amsg.msg.body.(*VOIPControl)
			client.PublishMessage(amsg.appid, ctrl)
		}
	}

	msg := &Message{cmd:MSG_PUBLISH, body:amsg}
	for c := range(s) {
		//不发送给自身
		if client == c {
			continue
		}
		c.wt <- msg
	}
}

func (client *Client) HandleSubscribeRoom(id *AppRoomID) {
	log.Infof("subscribe appid:%d room id:%d", id.appid, id.room_id)
	route := client.app_route.FindOrAddRoute(id.appid)
	route.AddRoomID(id.room_id)
}

func (client *Client) HandleUnsubscribeRoom(id *AppRoomID) {
	log.Infof("unsubscribe appid:%d room id:%d", id.appid, id.room_id)
	route := client.app_route.FindOrAddRoute(id.appid)
	route.RemoveRoomID(id.room_id)
}

func (client *Client) HandlePublishRoom(amsg *AppMessage) {
	log.Infof("publish room message appid:%d room id:%d cmd:%s", amsg.appid, amsg.receiver, Command(amsg.msg.cmd))
	receiver := &AppRoomID{appid:amsg.appid, room_id:amsg.receiver}
	s := FindRoomClientSet(receiver)

	msg := &Message{cmd:MSG_PUBLISH_ROOM, body:amsg}
	for c := range(s) {
		//不发送给自身
		if client == c {
			continue
		}
		log.Info("publish room message")
		c.wt <- msg
	}
}



func (client *Client) Write() {
	seq := 0
	for {
		msg := <-client.wt
		if msg == nil {
			client.close()
			log.Infof("client socket closed")
			break
		}
		seq++
		msg.seq = seq
		client.send(msg)
	}
}

func (client *Client) Run() {
	go client.Write()
	go client.Read()
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
	runtime.GOMAXPROCS(runtime.NumCPU())
	flag.Parse()
	if len(flag.Args()) == 0 {
		fmt.Println("usage: im config")
		return
	}

	config = read_route_cfg(flag.Args()[0])
	log.Infof("listen:%s redis:%s\n", config.listen, config.redis_address)

	redis_pool = NewRedisPool(config.redis_address, config.redis_password)

	ListenClient()
}
