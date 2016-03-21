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
import "fmt"
import "flag"
import "time"
import "runtime"
import "github.com/garyburd/redigo/redis"
import log "github.com/golang/glog"
import "database/sql"
import _ "github.com/go-sql-driver/mysql"
import "math/rand"

var server_id string

//storage server
var storage_channels []*StorageChannel
var storage_channels_map map[string]*StorageChannel

//route server
var route_channels []*Channel
var route_channels_map map[string]*Channel

//storage pool
var storage_pools []*StorageConnPool
var storage_pools_map map[string]*StorageConnPool

var mutex sync.Mutex
var config_path string

var route *Route
var redis_pool *redis.Pool
var config *Config
var server_summary *ServerSummary

func init() {
	route = NewRoute()
	server_summary = NewServerSummary()
}

func handle_client(conn net.Conn) {
	log.Infoln("handle_client")
	client := NewClient(conn)
	client.Run()
}

func Listen(f func(net.Conn), port int) {
	TCPService(fmt.Sprintf("0.0.0.0:%d", port), f)

}
func ListenClient() {
	Listen(handle_client, config.port)
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

func GetStorageConnPool(uid int64) *StorageConnPool {
	mutex.Lock()
	defer mutex.Unlock()
	
	index := uid%int64(len(storage_pools))
	return storage_pools[index]
}

func GetGroupStorageConnPool(gid int64) *StorageConnPool {
	mutex.Lock()
	defer mutex.Unlock()
	
	index := gid%int64(len(storage_pools))
	return storage_pools[index]
}

func GetRouteChannel() *Channel{
	mutex.Lock()
	defer mutex.Unlock()
	
	rand.Seed(time.Now().Unix())
	index := rand.Intn(len(route_channels))
	return route_channels[index]
}

func SaveGroupMessage(appid int64, gid int64, device_id int64, m *Message) (int64, error) {
	log.Infof("save group message:%d %d\n", appid, gid)
	storage_pool := GetGroupStorageConnPool(gid)
	storage, err := storage_pool.Get()
	if err != nil {
		log.Error("connect storage err:", err)
		return 0, err
	}
	defer storage_pool.Release(storage)

	sae := &SAEMessage{}
	sae.msg = m
	sae.appid = appid
	sae.receiver = gid
	sae.device_id = device_id

	msgid, err := storage.SaveAndEnqueueGroupMessage(sae)
	if err != nil {
		log.Error("saveandequeue message err:", err)
		return 0, err
	}
	return msgid, nil
}

func SaveMessage(appid int64, uid int64, device_id int64, m *Message) (int64, error) {
	storage_pool := GetStorageConnPool(uid)
	storage, err := storage_pool.Get()
	if err != nil {
		log.Error("connect storage err:", err)
		return 0, err
	}
	defer storage_pool.Release(storage)

	sae := &SAEMessage{}
	sae.msg = m
	sae.appid = appid
	sae.receiver = uid
	sae.device_id = device_id

	msgid, err := storage.SaveAndEnqueueMessage(sae)
	if err != nil {
		log.Error("saveandequeue message err:", err)
		return 0, err
	}
	return msgid, nil
}

func Send0Message(appid int64, uid int64, msg *Message) bool {
	amsg := &AppMessage{appid:appid, receiver:uid, msgid:0, msg:msg}
	SendAppMessage(amsg)
	return true
}

func SendAppMessage(amsg *AppMessage) bool {
	channel := GetRouteChannel()
	channel.Publish(amsg)

	return true
}

func DispatchAppMessage(amsg *AppMessage) {
	log.Info("dispatch app message:", Command(amsg.msg.cmd))

	clients := route.FindClientSet(amsg.receiver)
	if len(clients) == 0 {
		log.Warningf("can't dispatch app message, appid:%d uid:%d cmd:%s", amsg.appid, amsg.receiver, Command(amsg.msg.cmd))
		return
	}
	for c, _ := range(clients) {
		//自己在同一台设备上发出的消息，不再发送回去
		if amsg.msg.cmd == MSG_IM || amsg.msg.cmd == MSG_GROUP_IM || amsg.msg.cmd == MSG_ROOM_IM {
			m := amsg.msg.body.(*IMMessage)
			if m.sender == amsg.receiver && amsg.device_id == c.device_ID {
				continue
			}
		}

		if amsg.msgid > 0 {
			c.ewt <- &EMessage{msgid:amsg.msgid, msg:amsg.msg}
		} else {
			c.wt <- amsg.msg
		}
	}
}

func RouteMessage(msg *Message) {
	log.Info("route message:", Command(msg.cmd))
	
	channel := GetRouteChannel()
	channel.PublishMessage(msg)
}

func DialStorageFun(addr string) func()(*StorageConn, error) {
	f := func() (*StorageConn, error){
		storage := NewStorageConn()
		err := storage.Dial(addr)
		if err != nil {
			log.Error("connect storage err:", err)
			return nil, err
		}
		
		msg := &ServerID{
			serverid : config.server_id,
		}
		
		m := &Message{}
		m.cmd = MSG_SERVER_REGISTER
		m.body = msg
	
		SendMessage(storage.conn, m)
		return storage, nil
	}
	return f
}

//动态维护配置的storage, route节点
func ConfigLoop() {
	for {
		time.Sleep(time.Duration(60)*time.Second)
		
		//读配置文件
		cfg := read_cfg(config_path)
		if cfg == nil {
			continue
		}
		
		need := false
		//计算storage有节点下掉或者新节点则重新配置
		if len(cfg.storage_addrs) < len(config.storage_addrs) {
			//节点下掉
			need = true
		} else {
			//判断有无新增节点
			for _, addr := range cfg.storage_addrs {
				if _, ok := storage_pools_map[addr]; !ok {
					need = true
					break;
				}
			}
		}
		
		if need {
			mutex.Lock()
			
			storage_pools_new := make([]*StorageConnPool, 0)
			storage_pools_map_new := make(map[string]*StorageConnPool)
			storage_channels_new := make([]*StorageChannel, 0)
			storage_channels_map_new := make(map[string]*StorageChannel)
		
			for _, addr := range(cfg.storage_addrs) {
				if pool, ok := storage_pools_map[addr]; ok {
					storage_pools_new = append(storage_pools_new, pool)
					storage_pools_map_new[addr] = pool
				} else {
					f := DialStorageFun(addr)
					pool := NewStorageConnPool(100, 500, 600 * time.Second, f) 
					storage_pools_new = append(storage_pools_new, pool)
					storage_pools_map_new[addr] = pool
				}
				
				if pool, ok := storage_channels_map[addr]; ok {
					storage_channels_new = append(storage_channels_new, pool)
					storage_channels_map_new[addr] = pool
				} else {
					sc := NewStorageChannel(addr, RouteMessage)
					sc.Start()
					sc.Register()
					storage_channels_new = append(storage_channels_new, sc)
					storage_channels_map_new[addr] = sc
				}
			}
			
			storage_pools = storage_pools_new
			storage_pools_map = storage_pools_map_new
			
			storage_channels = storage_channels_new
			storage_channels_map = storage_channels_map_new
			
			config.storage_addrs = cfg.storage_addrs
			
			mutex.Unlock()
		}
	
		need = false
		//计算route有节点下掉或者新节点则重新配置
		if len(cfg.route_addrs) < len(config.route_addrs) {
			//节点下掉
			need = true
		} else {
			//判断有无新增节点
			for _, addr := range cfg.route_addrs {
				if _, ok := route_channels_map[addr]; !ok {
					need = true
					break;
				}
			}
		}
		
		if need {
			mutex.Lock()
			
			route_channels_new := make([]*Channel, 0)
			route_channels_map_new := make(map[string]*Channel)
		
			for _, addr := range(cfg.route_addrs) {
				if pool, ok := route_channels_map[addr]; ok {
					route_channels_new = append(route_channels_new, pool)
					route_channels_map_new[addr] = pool
				} else {
					channel := NewChannel(addr, DispatchAppMessage)
					channel.Start()
					channel.Register()
					route_channels_new = append(route_channels_new, channel)
					route_channels_map_new[addr] = channel
				}
			}
			
			route_channels = route_channels_new
			route_channels_map = route_channels_map_new
			
			config.route_addrs = cfg.route_addrs
			
			mutex.Unlock()
		}		
	}
}

func LoadDBData() {
	db, err := sql.Open("mysql", config.mysqldb_appdatasource)
	if err != nil {
		log.Info("error:", err)
		return
	}
	defer db.Close()
	
	//加载好友数据
	OpLoadAllFriends(db)
	
	//加载黑名单数据
	OpLoadAllBlacks(db)
	
	//加载群组数据
	OpLoadAllGroup(db)
}


func main() {
	runtime.GOMAXPROCS(runtime.NumCPU())
	flag.Parse()
	if len(flag.Args()) == 0 {
		fmt.Println("usage: im config")
		return
	}

	config_path = flag.Args()[0]
	config = read_cfg(config_path)
	if config == nil {
		return
	}
	
	server_id = config.server_id
	
	log.Infof("port:%d redis address:%s\n",
		config.port,  config.redis_address)

	log.Info("storage addresses:", config.storage_addrs)
	log.Info("route addressed:", config.route_addrs)

	redis_pool = NewRedisPool(config.redis_address, config.redis_password)

	storage_pools = make([]*StorageConnPool, 0)
	storage_pools_map = make(map[string]*StorageConnPool)
	for _, addr := range(config.storage_addrs) {
		f := DialStorageFun(addr)
		pool := NewStorageConnPool(100, 500, 600 * time.Second, f) 
		storage_pools = append(storage_pools, pool)
		storage_pools_map[addr] = pool
	}

	storage_channels = make([]*StorageChannel, 0)
	storage_channels_map = make(map[string]*StorageChannel)
	for _, addr := range(config.storage_addrs) {
		sc := NewStorageChannel(addr, RouteMessage)
		sc.Start()
		sc.Register()
		storage_channels = append(storage_channels, sc)
		storage_channels_map[addr] = sc
	}

	route_channels = make([]*Channel, 0)
	route_channels_map = make(map[string]*Channel)
	for _, addr := range(config.route_addrs) {
		channel := NewChannel(addr, DispatchAppMessage)
		channel.Start()
		channel.Register()
		route_channels = append(route_channels, channel)
		route_channels_map[addr] = channel
	}
	
	LoadDBData()
	
	go ConfigLoop()

	go StartSocketIO(config.socket_io_address)
	ListenClient()
	Wait()
}
