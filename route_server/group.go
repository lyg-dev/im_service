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
	"time"
	"fmt"
)
import "database/sql"
import _ "github.com/go-sql-driver/mysql"
import log "github.com/golang/glog"
import "github.com/garyburd/redigo/redis"

func OpGetGroupMembers(gid int64) []int64 {
	conn := redis_pool.Get()
	defer conn.Close()
	
	uids := make([]int64, 4, 10)
	
	key := fmt.Sprintf("group_members_%d", gid)
	members, err := redis.Ints(conn.Do("SMEMBERS", key))
	if err != nil {
		return uids
	}
	
	if len(members) == 0 {
		ms, err := LoadGroupMember(gid)
		if err != nil {
			return uids
		}
		
		for _, m := range ms {
			_, err = conn.Do("SADD", key, m)
			if err != nil {
				log.Infoln(err)
			}
		}
		
		return ms
	}
	
	for _, member := range members {
		uids = append(uids, int64(member))
	}
	
	return uids
}

func OpGetRoomMembers(gid int64) []int64 {
	conn := redis_pool.Get()
	defer conn.Close()
	
	uids := make([]int64, 4, 10)
	key := fmt.Sprintf("room_members_%d", gid)
	members, err := redis.Ints(conn.Do("SMEMBERS", key))
	if err != nil {
		return uids
	}
	
	for _, member := range members {
		uids = append(uids, int64(member))
	}
	
	return uids
}

func OpAddRoomMember(gid int64, uid int64) bool {
	conn := redis_pool.Get()
	defer conn.Close()
	
	key := fmt.Sprintf("room_members_%d", gid)
	_, err := conn.Do("SADD", key, uid)
	if err != nil {
		log.Infoln(err)
		return false
	}
	
	return true
}

func OpRemoveRoomMember(gid int64, uid int64) bool {
	conn := redis_pool.Get()
	defer conn.Close()
	
	key := fmt.Sprintf("room_members_%d", gid)
	_, err := conn.Do("SREM", key, uid)
	if err != nil {
		log.Infoln(err)
		return false
	}
	
	return true
}

func OpAddGroupMember(gid int64, uid int64, isOwner int) bool {
	if !AddGroupMember(gid, uid, isOwner) {
		return false
	}
	
	conn := redis_pool.Get()
	defer conn.Close()
	
	key := fmt.Sprintf("group_members_%d", gid)
	_, err := conn.Do("SADD", key, uid)
	if err != nil {
		log.Infoln(err)
		return false
	}
	
	return true
}

func OpRemoveGroupMember(gid int64, uid int64) bool {
	if !RemoveGroupMember(gid, uid) {
		return false
	}
	
	conn := redis_pool.Get()
	defer conn.Close()
	
	key := fmt.Sprintf("group_members_%d", gid)
	_, err := conn.Do("SREM", key, uid)
	if err != nil {
		log.Infoln(err)
		return false
	}
	
	return true
}

func AddGroupMember(group_id int64, uid int64, isOwner int) bool {
	db, err := sql.Open("mysql", config.mysqldb_appdatasource)
	if err != nil {
		log.Info("error:", err)
		return false
	}
	defer db.Close()
	
	var stmt1, stmt2 *sql.Stmt

	tx, err := db.Begin()
	if err != nil {
		log.Info("error:", err)
		return false
	}

	sql := fmt.Sprintf("INSERT INTO `group_members_0%d` ( `group_id`, `user_id`, `create_time`, `update_time`) select '%d', %d, %d, %d from dual where not exists(select * from group_members_0%d where group_id='%d' and user_id=%d)",
			group_id % 10, group_id, uid, time.Now().Unix(), time.Now().Unix(), group_id % 10, group_id, uid)
	stmt1, err = tx.Prepare(sql)
	if err != nil {
		log.Info("error:", err)
		goto ROLLBACK
	}
	defer stmt1.Close()
	_, err = stmt1.Exec()
	if err != nil {
		log.Info("error:", err)
		goto ROLLBACK
	}

	sql = fmt.Sprintf("INSERT INTO `user_groups_0%d` ( `group_id`, `user_id`, `isOwner`, `type`) select '%d', %d, %d, %d from dual where not exists(select * from user_groups_0%d where type=1 and group_id='%d' and user_id=%d)",
			uid % 10, group_id, uid, isOwner, 1, uid % 10, group_id, uid)
	stmt2, err = tx.Prepare(sql)
	if err != nil {
		log.Info("error:", err)
		goto ROLLBACK
	}
	defer stmt2.Close()
	_, err = stmt2.Exec()
	if err != nil {
		log.Info("error:", err)
		goto ROLLBACK
	}

	tx.Commit()
	return true

ROLLBACK:
	tx.Rollback()
	return false
}

func RemoveGroupMember(group_id int64, uid int64) bool {
	db, err := sql.Open("mysql", config.mysqldb_appdatasource)
	if err != nil {
		log.Info("error:", err)
		return false
	}
	defer db.Close()
	
	var stmt1, stmt2 *sql.Stmt

	tx, err := db.Begin()
	if err != nil {
		log.Info("error:", err)
		return false
	}

	sql := fmt.Sprintf("delete from group_members_0%d where group_id=? and user_id=?", group_id % 10);
	stmt1, err = tx.Prepare(sql)
	if err != nil {
		log.Info("error:", err)
		goto ROLLBACK
	}
	defer stmt1.Close()
	_, err = stmt1.Exec(group_id, uid)
	if err != nil {
		log.Info("error:", err)
		goto ROLLBACK
	}

	sql = fmt.Sprintf("delete from user_groups_0%d where type=1 and group_id=? and user_id=?", uid % 10);
	stmt2, err = tx.Prepare(sql)
	if err != nil {
		log.Info("error:", err)
		goto ROLLBACK
	}
	defer stmt2.Close()
	_, err = stmt2.Exec(group_id, uid)
	if err != nil {
		log.Info("error:", err)
		goto ROLLBACK
	}

	tx.Commit()
	return true

ROLLBACK:
	tx.Rollback()
	return false
}

func LoadGroupMember(group_id int64) ([]int64, error) {
	db, err := sql.Open("mysql", config.mysqldb_appdatasource)
	if err != nil {
		log.Info("error:", err)
		return nil, err
	}
	defer db.Close()
	
	sql := fmt.Sprintf("SELECT user_id FROM group_members_0%d WHERE group_id=?", group_id % 10)
	stmtIns, err := db.Prepare(sql)
	if err != nil {
		log.Info("error:", err)
		return nil, err
	}

	defer stmtIns.Close()
	members := make([]int64, 0, 4)
	rows, err := stmtIns.Query(group_id)
	for rows.Next() {
		var uid int64
		rows.Scan(&uid)
		members = append(members, uid)
	}
	return members, nil
}
