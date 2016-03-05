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
	"sync"
	"fmt"
)
import "strconv"
import "strings"
import "time"
import "database/sql"
import "github.com/garyburd/redigo/redis"
import _ "github.com/go-sql-driver/mysql"
import log "github.com/golang/glog"
import "im_service/common"

type UserManager struct {
	//好友列表
	friends map[int64]common.IntSet
	//黑名单
	blacks map[int64]common.IntSet
	mutex sync.Mutex
}

func NewUserManager() *UserManager {
	m := new(UserManager)
	m.friends = make(map[int64]common.IntSet)
	m.blacks = make(map[int64]common.IntSet)
	return m
}

func (user_manager *UserManager) IsFriend(uid int64, fid int64) bool {
	if _, ok := user_manager.friends[uid]; ok {
		return user_manager.friends[uid].IsMember(fid)
	}
	
	return false
}

func (user_manager *UserManager) IsBlack(uid int64, bid int64) bool {
	if _, ok := user_manager.blacks[uid]; ok {
		return user_manager.blacks[uid].IsMember(bid)
	}
	
	return false
}

func (user_manager *UserManager) PubFriendAdd(uid int64, fid int64) {
	conn := redis_pool.Get()
	defer conn.Close()
	
	msg := fmt.Sprintf("%d,%d", uid, fid);
	_, err := conn.Do("PUBLISH", "friend_add", msg)
	
	if err != nil {
		log.Info("friend add pub error:", err)
	}
	
	msg = fmt.Sprintf("%d,%d", fid, uid);
	_, err = conn.Do("PUBLISH", "friend_add", msg)
	
	if err != nil {
		log.Info("friend add pub error:", err)
	}
}

func (user_manager *UserManager) PubFriendRemove(uid int64, fid int64) {
	conn := redis_pool.Get()
	defer conn.Close()
	
	msg := fmt.Sprintf("%d,%d", uid, fid);
	_, err := conn.Do("PUBLISH", "friend_remove", msg)
	
	if err != nil {
		log.Info("friend remove pub error:", err)
	}
	
	msg = fmt.Sprintf("%d,%d", fid, uid);
	_, err = conn.Do("PUBLISH", "friend_remove", msg)
	
	if err != nil {
		log.Info("friend remove pub error:", err)
	}
} 

func (user_manager *UserManager) PubBlackAdd(uid int64, bid int64) {
	conn := redis_pool.Get()
	defer conn.Close()
	
	msg := fmt.Sprintf("%d,%d", uid, bid);
	_, err := conn.Do("PUBLISH", "black_add", msg)
	
	if err != nil {
		log.Info("black add pub error:", err)
	}
}

func (user_manager *UserManager) PubBlackRemove(uid int64, bid int64) {
	conn := redis_pool.Get()
	defer conn.Close()
	
	msg := fmt.Sprintf("%d,%d", uid, bid);
	_, err := conn.Do("PUBLISH", "black_remove", msg)
	
	if err != nil {
		log.Info("black remove pub error:", err)
	}
}

func (user_manager *UserManager) PubGroupCreate(gid int64) {
	conn := redis_pool.Get()
	defer conn.Close()
	
	msg := fmt.Sprintf("%d,%d", gid, 1);
	_, err := conn.Do("PUBLISH", "group_create", msg)
	
	if err != nil {
		log.Info("group create error:", err)
	}
} 

func (user_manager *UserManager) PubGroupMemberAdd(gid int64, uid int64) {
	conn := redis_pool.Get()
	defer conn.Close()
	
	msg := fmt.Sprintf("%d,%d", gid, uid);
	_, err := conn.Do("PUBLISH", "group_member_add", msg)
	
	if err != nil {
		log.Info("group member add pub error:", err)
	}
}

func (user_manager *UserManager) HandleFriendAdd(data string) {
	db, err := sql.Open("mysql", config.mysqldb_appdatasource)
	if err != nil {
		log.Info("error:", err)
		return
	}
	defer db.Close()
	
	arr := strings.Split(data, ",")
	if len(arr) != 2 {
		log.Info("message error")
		return
	}
	uid, err := strconv.ParseInt(arr[0], 10, 64)
	if err != nil {
		log.Info("error:", err)
		return
	}
	fid, err := strconv.ParseInt(arr[1], 10, 64)
	if err != nil {
		log.Info("error:", err)
		return
	}
	
	user_manager.mutex.Lock()
	defer user_manager.mutex.Unlock()
	
	if _, ok := user_manager.friends[uid]; !ok {
		user_manager.friends[uid] = common.NewIntSet()
	}
	
	user_manager.friends[uid].Add(fid)
}

func (user_manager *UserManager) HandleFriendRemove(data string) {	
	arr := strings.Split(data, ",")
	if len(arr) != 2 {
		log.Info("message error")
		return
	}
	uid, err := strconv.ParseInt(arr[0], 10, 64)
	if err != nil {
		log.Info("error:", err)
		return
	}
	fid, err := strconv.ParseInt(arr[1], 10, 64)
	if err != nil {
		log.Info("error:", err)
		return
	}
	
	user_manager.mutex.Lock()
	defer user_manager.mutex.Unlock()
	
	if _, ok := user_manager.friends[uid]; ok {
		user_manager.friends[uid].Remove(fid)
	}
}

func (user_manager *UserManager) HandleBlackAdd(data string) {
	db, err := sql.Open("mysql", config.mysqldb_appdatasource)
	if err != nil {
		log.Info("error:", err)
		return
	}
	defer db.Close()
	
	arr := strings.Split(data, ",")
	if len(arr) != 2 {
		log.Info("message error")
		return
	}
	uid, err := strconv.ParseInt(arr[0], 10, 64)
	if err != nil {
		log.Info("error:", err)
		return
	}
	bid, err := strconv.ParseInt(arr[1], 10, 64)
	if err != nil {
		log.Info("error:", err)
		return
	}
	
	user_manager.mutex.Lock()
	defer user_manager.mutex.Unlock()
	
	if _, ok := user_manager.blacks[uid]; !ok {
		user_manager.blacks[uid] = common.NewIntSet()
	}
	
	user_manager.blacks[uid].Add(bid)
}

func (user_manager *UserManager) HandleBlackRemove(data string) {
	arr := strings.Split(data, ",")
	if len(arr) != 2 {
		log.Info("message error")
		return
	}
	uid, err := strconv.ParseInt(arr[0], 10, 64)
	if err != nil {
		log.Info("error:", err)
		return
	}
	bid, err := strconv.ParseInt(arr[1], 10, 64)
	if err != nil {
		log.Info("error:", err)
		return
	}
	
	user_manager.mutex.Lock()
	defer user_manager.mutex.Unlock()
	
	if _, ok := user_manager.blacks[uid]; ok {
		user_manager.blacks[uid].Remove(bid)
	}
}

func (user_manager *UserManager) Reload() {
	db, err := sql.Open("mysql", config.mysqldb_appdatasource)
	if err != nil {
		log.Info("error:", err)
		return
	}
	defer db.Close()

	friends, err := LoadAllFriends(db)
	if err != nil {
		log.Info("error:", err)
		return
	}
	
	blacks, err := LoadAllBlacks(db)
	if err != nil {
		log.Info("error:", err)
		return
	}

	user_manager.mutex.Lock()
	defer user_manager.mutex.Unlock()
	user_manager.friends = friends
	user_manager.blacks = blacks
}

func (user_manager *UserManager) RunOnce() bool {
	c, err := redis.Dial("tcp", config.redis_address)
	if err != nil {
		log.Info("dial redis error:", err)
		return false
	}
	psc := redis.PubSubConn{c}
	psc.Subscribe("friend_add", "friend_remove", "black_add", "black_remove")
	group_manager.Reload()
	for {
		switch v := psc.Receive().(type) {
		case redis.Message:
			if v.Channel == "friend_add" {
				user_manager.HandleFriendAdd(string(v.Data))
			} else if v.Channel == "friend_remove" {
				user_manager.HandleFriendRemove(string(v.Data))
			} else if v.Channel == "black_add" {
				user_manager.HandleBlackAdd(string(v.Data))
			} else if v.Channel == "black_remove" {
				user_manager.HandleBlackRemove(string(v.Data))
			} else {
				log.Infof("%s: message: %s\n", v.Channel, v.Data)
			}
		case redis.Subscription:
			log.Infof("%s: %s %d\n", v.Channel, v.Kind, v.Count)
		case error:
			log.Info("error:", v)
			return true
		}
	}
}

func (user_manager *UserManager) Run() {
	nsleep := 1
	for {
		connected := user_manager.RunOnce()
		if !connected {
			nsleep *= 2
			if nsleep > 60 {
				nsleep = 60
			}
		} else {
			nsleep = 1
		}
		time.Sleep(time.Duration(nsleep) * time.Second)
	}
}

func (user_manager *UserManager) Start() {
	user_manager.Reload()
	go user_manager.Run()
}
