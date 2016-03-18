package main

import (
	"fmt"
)

import "github.com/garyburd/redigo/redis"

//获取用户客户端设备连接机器
func GetUserServers(uid int64) []string {
	conn := redis_pool.Get()
	defer conn.Close()
	
	key := fmt.Sprintf("user_servers_%d", uid)
	servers, err := redis.Strings(conn.Do("SMEMBERS", key))
	if err != nil {
		return nil
	}
	
	return servers
}