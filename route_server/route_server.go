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
import (
	"net"
	"sync"
)
import "runtime"
import "flag"
import "fmt"
import "time"
import "bytes"
import "encoding/binary"
import log "github.com/golang/glog"
import "github.com/garyburd/redigo/redis"

var config *RouteConfig
var redis_pool *redis.Pool
var clients map[string]*Client
var clients_mutex sync.Mutex

type Client struct {
	wt     chan *Message
	
	serverId string
	conn   *net.TCPConn
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

func (client *Client) HandleMessage(msg *Message) {
	log.Info("msg cmd:", Command(msg.cmd))
	switch msg.cmd {
	case MSG_SERVER_REGISTER:
		client.HandleRegister(msg.body.(*ServerID))
	case MSG_PUBLISH:
		client.HandlePublish(msg.body.(*AppMessage))
	case MSG_PUBLISH_GROUP:
		client.HandlePublishGroup(msg.body.(*AppMessage))
	case MSG_PUBLISH_ROOM:
		client.HandlePublishRoom(msg.body.(*AppMessage))
	default:
		log.Warning("unknown message cmd:", msg.cmd)
	}
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

func (client *Client) HandleRegister(id *ServerID) {
	clients_mutex.Lock()
	defer clients_mutex.Unlock()
	
	if _, ok := clients[id.serverid]; ok {
		return
	}
	
	client.serverId = id.serverid
	clients[id.serverid] = client
}


func (client *Client) HandlePublish(amsg *AppMessage) {
	log.Infof("publish message appid:%d uid:%d msgid:%d cmd:%s", amsg.appid, amsg.receiver, amsg.msgid, Command(amsg.msg.cmd))
	receiver := amsg.receiver
	
	servers := GetUserServers(receiver)

	if servers == nil || len(servers) == 0 {
		//用户不在线,推送消息到终端, 苹果apns
		return
	}

	msg := &Message{cmd:MSG_PUBLISH, body:amsg}
	
	clients_mutex.Lock()
	defer clients_mutex.Unlock()
	
	for _, serverId := range servers {
		if c, ok := clients[serverId]; ok {
			c.wt <- msg
		}
	}
}

func (client *Client) HandlePublishRoom(amsg *AppMessage) {
	log.Infof("publish room message appid:%d room id:%d cmd:%s", amsg.appid, amsg.receiver, Command(amsg.msg.cmd))
	receiver := amsg.receiver
	
	members := OpGetRoomMembers(receiver)
	
	for _, member := range members {		
		servers := GetUserServers(member)
		
		if servers == nil || len(servers) == 0 {
			return
		}
	
		amsg1 := &AppMessage{
			appid : amsg.appid,
			receiver : member,
			msgid : amsg.msgid,
			device_id : amsg.device_id,
			msg : amsg.msg,
		}
		msg := &Message{cmd:MSG_PUBLISH_ROOM, body: amsg1}
		
		clients_mutex.Lock()
		defer clients_mutex.Unlock()
		
		for _, serverId := range servers {
			if c, ok := clients[serverId]; ok {
				c.wt <- msg
			}
		}
	}
}

func (client *Client) HandlePublishGroup(amsg *AppMessage) {
	log.Infof("publish group message appid:%d group id:%d cmd:%s", amsg.appid, amsg.receiver, Command(amsg.msg.cmd))
	receiver := amsg.receiver

	members := OpGetGroupMembers(receiver)
	
	for _, member := range members {		
		servers := GetUserServers(member)
		
		if servers == nil || len(servers) == 0 {
			//用户不在线,推送消息到终端, 苹果apns
			return
		}
	
		amsg1 := &AppMessage{
			appid : amsg.appid,
			receiver : member,
			msgid : amsg.msgid,
			device_id : amsg.device_id,
			msg : amsg.msg,
		}
		msg := &Message{cmd:MSG_PUBLISH_GROUP, body: amsg1}
		
		clients_mutex.Lock()
		defer clients_mutex.Unlock()
		
		for _, serverId := range servers {
			if c, ok := clients[serverId]; ok {
				c.wt <- msg
			}
		}
	}
}

func RemoveClient(serverId string) {
	clients_mutex.Lock()
	defer clients_mutex.Unlock()
	
	delete(clients, serverId)
}

func (client *Client) Write() {
	seq := 0
	for {
		msg := <-client.wt
		if msg == nil {
			RemoveClient(client.serverId)
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
	
	clients = make(map[string]*Client)

	ListenClient()
}
