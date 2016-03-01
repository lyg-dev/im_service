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

import "fmt"
import "time"
import log "github.com/golang/glog"
import "github.com/garyburd/redigo/redis"
import "errors"
import "database/sql"
import _ "github.com/go-sql-driver/mysql"
import "encoding/json"

func LoadUserAccessToken(token string) (int64, int64, string, error) {
	conn := redis_pool.Get()
	defer conn.Close()

	key := fmt.Sprintf("access_token_%s", token)
	var uid int64
	var appid int64
	var uname string

	exists, err := redis.Bool(conn.Do("EXISTS", key))
	if err != nil {
		return 0, 0, "", err
	}
	if !exists {
		appid, uid, uname, err = LoadUserInfoByAccessToken(token)
		if err == nil {
			conn.Do("HMSET", key, "user_id", uid, "app_id", appid, "user_name", uname)
			return appid, uid, uname, nil
		}
		return 0, 0, "", errors.New("token non exists")
	}

	reply, err := redis.Values(conn.Do("HMGET", key, "user_id", "app_id", "user_name"))
	if err != nil {
		log.Info("hmget error:", err)
		return 0, 0, "", err
	}

	_, err = redis.Scan(reply, &uid, &appid, &uname)
	if err != nil {
		log.Warning("scan error:", err)
		return 0, 0, "", err
	}
	return appid, uid, uname, nil	
}

func LoadUserInfoByAccessToken(token string) (int64, int64, string, error) {
	db, err := sql.Open("mysql", config.mysqldb_appdatasource)
	if err != nil {
		log.Info("error:", err)
		return 0, 0, "", err
	}
	defer db.Close()
	
	stmt, err := db.Prepare("SELECT id, username FROM user_app WHERE access_token=?")
	if err != nil {
		log.Info("error:", err)
		return 0, 0, "", err
	}

	defer stmt.Close()
	
	var id int64
	var uname string
	err = stmt.QueryRow(token).Scan(&id, &uname)
	if err != nil {
		return 0, 0, "", err
	}
	
//	loadUserBasicDatas(db, id)
	
	return 1, id, uname, nil
}

func loadUserBasicDatas(db *sql.DB, uid int64) {
	conn := redis_pool.Get()
	defer conn.Close()
	
	key := fmt.Sprintf("im_user_friends_%s", uid)
	
	//加载用户好友列表
    sql := fmt.Sprintf("SELECT friend_id FROM user_friends_0%d WHERE user_id=?", uid % 10)
	stmt, err := db.Prepare(sql)
	if err == nil {
		rows, err := stmt.Query(uid)
		for rows.Next() {
			var fid int64
			rows.Scan(&fid)
			_, err = conn.Do("SADD", key, fid)
			if err != nil {
				log.Info("loadUserBasicDatas[add user friend to redis]error:", err)
			}
		}
	}
	
	key = fmt.Sprintf("im_user_blacks_%s", uid)
	//加载用户黑名单列表
	stmt, err = db.Prepare("SELECT blacks FROM user_blacks WHERE user_id=?")
	if err == nil {
		var blacksStr string
		err = stmt.QueryRow(uid).Scan(&blacksStr)
		if err == nil {
			var black_ids []int64
			err = json.Unmarshal([]byte(blacksStr), &black_ids)
			
			if err == nil {
				for _, bid := range black_ids {
					_, err = conn.Do("SADD", key, bid)
					if err != nil {
						log.Info("loadUserBasicDatas[add user black to redis]error:", err)
					}
				}
			}
		}
	}
	
	//加载用户群组列表
	key = fmt.Sprintf("im_user_groups_%s", uid)
	
	sql = fmt.Sprintf("SELECT group_id FROM user_groups_0%d WHERE type=1 AND user_id=?", uid % 10)
	
	stmt, err = db.Prepare(sql)
	if err == nil {
		rows, err := stmt.Query(uid)
		for rows.Next() {
			var gid int64
			rows.Scan(&gid)
			_, err = conn.Do("SADD", key, gid)
			if err != nil {
				log.Info("loadUserBasicDatas[add user group to redis]error:", err)
			}
		}
	}
	
	//加载聊天室列表
	key = fmt.Sprintf("im_user_rooms_%s", uid)
	
	sql = fmt.Sprintf("SELECT room_id_20302 FROM user_groups_0%d WHERE type=2 AND user_id=?", uid % 10)
	
	stmt, err = db.Prepare(sql)
	if err == nil {
		rows, err := stmt.Query(uid)
		for rows.Next() {
			var rid int64
			rows.Scan(&rid)
			_, err = conn.Do("SADD", key, rid)
			if err != nil {
				log.Info("loadUserBasicDatas[add user room to redis]error:", err)
			}
		}
	}
}

func CountUser(appid int64, uid int64) {
	conn := redis_pool.Get()
	defer conn.Close()

	key := fmt.Sprintf("statistics_users_%d", appid)
	_, err := conn.Do("PFADD", key, uid)
	if err != nil {
		log.Info("pfadd err:", err)
	}
}

func CountDAU(appid int64, uid int64) {
	conn := redis_pool.Get()
	defer conn.Close()
	
	now := time.Now()
	date := fmt.Sprintf("%d_%d_%d", now.Year(), int(now.Month()), now.Day())
	key := fmt.Sprintf("statistics_dau_%s_%d", date, appid)
	_, err := conn.Do("PFADD", key, uid)
	if err != nil {
		log.Info("pfadd err:", err)
	}
}

func SetUserUnreadCount(appid int64, uid int64, count int32) {
	conn := redis_pool.Get()
	defer conn.Close()

	key := fmt.Sprintf("users_%d_%d", appid, uid)
	_, err := conn.Do("HSET", key, "unread", count)
	if err != nil {
		log.Info("hset err:", err)
	}
}
