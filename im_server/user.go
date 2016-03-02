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
import "im_service/common"
import "strconv"

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
	
	return 1, id, uname, nil
}

func LoadAllFriends(db *sql.DB) (map[int64]common.IntSet, error) {
	
	//加载用户好友列表
	friends := make(map[int64]common.IntSet)
	
	i := 0
	for ; i < 10; i++ {
		sql := fmt.Sprintf("SELECT user_id, friend_id FROM user_friends_0%d", i)
		stmt, err := db.Prepare(sql)
		if err == nil {
			rows, _ := stmt.Query()
			for rows.Next() {
				var uid, fid int64
				rows.Scan(&uid, &fid)
				log.Infof("load friend from db: uid=%d, fid=%d", uid, fid)
				if _, ok := friends[uid]; !ok {
					friends[uid] = common.NewIntSet()
				}
				
				friends[uid].Add(fid)
			}
		} else {
			return nil, err
		}
	}
	
	return friends, nil
}

func LoadAllBlacks(db *sql.DB) (map[int64]common.IntSet, error) {
	
	//加载用户黑名单列表
	blacks := make(map[int64]common.IntSet)
	
	stmt, err := db.Prepare("SELECT user_id, blacks FROM user_blacks")
	if err != nil {
		return nil, err
	}
	
	rows, err := stmt.Query()
	for rows.Next() {
		var uid int64
		var blacksStr string
		
		err = rows.Scan(&uid, &blacksStr)
		if err == nil {
			var black_ids []string
			err = json.Unmarshal([]byte(blacksStr), &black_ids)
			
			if err == nil {
				for _, b := range black_ids {
					bid, _ := strconv.ParseInt(b, 10, 64)
					if _, ok := blacks[uid]; !ok {
						blacks[uid] = common.NewIntSet()
					}
					log.Infof("load black from db: uid=%d, bid=%d", uid, bid)
					blacks[uid].Add(bid)
				}
			} else {
				log.Errorf("1 load black error: %s", err)
			}
		} else {
			log.Errorf("2 load black error: %s", err)
		}
	}
		
	return blacks, nil
}

func FriendAdd(db *sql.DB, uid int64, fid int64) bool {
	sql := fmt.Sprintf("INSERT INTO `user_friends_0%d` ( `user_id`, `friend_id`, `create_time`) select %d, %d, %d from dual where not exists(select * from user_friends_0%d where user_id=%d and friend_id=%d)",
			uid % 10, uid, fid, time.Now().Unix(), uid % 10, uid, fid)
	stmtIns, err := db.Prepare(sql)
	if err != nil {
		log.Info("error:", err)
		return false
	}
	defer stmtIns.Close()
	_, err = stmtIns.Exec()
	if err != nil {
		log.Info("error:", err)
		return false
	}
	return true
}

func FriendRemove(db *sql.DB, uid int64, fid int64) bool {
	sql := fmt.Sprintf("delete from user_friends_0%d where user_id=? and friend_id=?", uid % 10);
	stmtIns, err := db.Prepare(sql)
	if err != nil {
		log.Info("error:", err)
		return false
	}
	defer stmtIns.Close()
	_, err = stmtIns.Exec(uid, fid)
	if err != nil {
		log.Info("error:", err)
		return false
	}
	return true
}

func BlackAdd(db *sql.DB, uid int64, bid int64) bool {
	stmt, err := db.Prepare("SELECT blacks FROM user_blacks WHERE user_id=?")
	if err != nil {
		return false
	}
	defer stmt.Close()
	
	insert := 0;
	blacks := make([]string, 0, 4)
	var blacksStr string
	err = stmt.QueryRow(uid).Scan(&blacksStr)
	if err == sql.ErrNoRows {
		insert = 1;
		blacks = append(blacks, strconv.FormatInt(bid, 10))
	} else if err != nil {
		return false
	} else {
		err = json.Unmarshal([]byte(blacksStr), &blacks)
		if err == nil {
			set := common.NewIntSet()
			for _, b := range blacks {
				black_id, _ := strconv.ParseInt(b, 10, 64)
				set.Add(black_id)
			}
			
			if !set.IsMember(bid) {
				blacks = append(blacks, strconv.FormatInt(bid, 10))
			}
		} else {
			blacks = append(blacks, strconv.FormatInt(bid, 10))
		}
	}
	
	bs, err := json.Marshal(blacks)
	if err != nil {
		return false
	}
	
	blacksStr = string(bs)
	
	if insert == 1 {
		stmt, err := db.Prepare("INSERT INTO user_blacks (user_id, blacks, update_time) VALUES (?, ?, ?)")
		if err != nil {
			return false
		}
		stmt.Exec(uid, blacksStr, time.Now().Unix())
	} else {
		stmt, err := db.Prepare("UPDATE user_blacks SET blacks=?, update_time=? WHERE user_id=?")
		if err != nil {
			return false
		}
		stmt.Exec(blacksStr, time.Now().Unix(), uid)
	}
	
	return true
}

func BlackRemove(db *sql.DB, uid int64, bid int64) bool {
	stmt, err := db.Prepare("SELECT blacks FROM user_blacks WHERE user_id=?")
	if err != nil {
		return false
	}
	defer stmt.Close()
	
	var blacksStr string
	err = stmt.QueryRow(uid).Scan(&blacksStr)
	if err == sql.ErrNoRows {
		return true
	} else if err != nil {
		return false
	}
	
	blacks := make([]string, 0, 4)
	_ = json.Unmarshal([]byte(blacksStr), &blacks)
	
	leftBlacks := make([]string, 0, 4)
	b := strconv.FormatInt(bid, 10)
	for _, black_id := range blacks {
		
		if black_id == b {
			continue
		}
		
		leftBlacks = append(leftBlacks, black_id)
	}
	
	bs, err := json.Marshal(leftBlacks)
	if err != nil {
		return false
	}
	
	blacksStr = string(bs)
	
	stmt, err = db.Prepare("UPDATE user_blacks SET blacks=?, update_time=? WHERE user_id=?")
	if err != nil {
		return false
	}
	stmt.Exec(blacksStr, time.Now().Unix(), uid)
	
	return true
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
