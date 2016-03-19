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

type Group struct {
	gid     int64
	is_private int
	is_allow_invite int
	title	string
	desc	string
	owner int64
	gouhao int
}


func OpCreateGroup(db *sql.DB, gid int64, title string, desc string, is_private int, is_allow_invite int, owner int64, gouhao int) bool {
	conn := redis_pool.Get()
	defer conn.Close()
	
	if !CreateGroup(db, gid, title, desc, is_private, is_allow_invite, owner, gouhao) {
		return false
	}
	
	key := fmt.Sprintf("group_%d", gid)
	_, err := conn.Do("HMSET", key, "title", title, "desc", desc, "is_private", is_private, "is_allow_invite", is_allow_invite, "owner", owner, "gouhao", gouhao)
	if err != nil {
		log.Infoln(err)
		return false
	}
	
	return true
}

func OpGetGroup(gid int64) *Group{
	conn := redis_pool.Get()
	defer conn.Close()
	
	key := fmt.Sprintf("group_%d", gid)
	reply, err := redis.Values(conn.Do("HMGET", key, "title", "desc", "is_private", "is_allow_invite", "owner", "gouhao"))
	if err != nil {
		log.Info("hmget error:", err)
		return nil
	}

	var title string
	var desc string
	var is_private int
	var is_allow_invite int
	var owner int64
	var gouhao int
	_, err = redis.Scan(reply, &title, &desc, &is_private, &is_allow_invite, &owner, &gouhao)
	if err != nil {
		log.Warning("scan error:", err)
		return nil
	}
	
	return &Group{
		gid: gid,
		title : title,
		is_private : is_private,
		is_allow_invite: is_allow_invite,
		owner: owner,
		gouhao:gouhao,
	}
}

func OpDelGroup(db *sql.DB, gid int64) bool {
	conn := redis_pool.Get()
	defer conn.Close()
	
	if !DeleteGroup(db, gid) {
		return false
	}
	
	key := fmt.Sprintf("group_%d", gid)
	_, err := conn.Do("DEL", key)
	if err != nil {
		log.Warning("del error:", err)
		return true
	}
	
	key = fmt.Sprintf("group_members_%d", gid)
	_, err = conn.Do("DEL", key)
	if err != nil {
		log.Warning("del error:", err)
		return true
	}
	
	return true
}

func OpGetGroupMemberNumber(gid int64) int {
	conn := redis_pool.Get()
	defer conn.Close()
	
	key := fmt.Sprintf("group_members_%d", gid)
	number, err := redis.Int(conn.Do("SCARD", key))
	if err != nil {
		return 0
	}
	
	return number
}

func OpIsGroupMember(gid int64, uid int64) bool {
	conn := redis_pool.Get()
	defer conn.Close()
	
	key := fmt.Sprintf("group_members_%d", gid)
	isMember, err := redis.Bool(conn.Do("SISMEMBER", key, uid))
	if err != nil {
		return false
	}
	
	return isMember
}

func OpGetGroupMembers(gid int64) []int64 {
	conn := redis_pool.Get()
	defer conn.Close()
	
	uids := make([]int64, 4, 10)
	
	key := fmt.Sprintf("group_members_%d", gid)
	members, err := redis.Ints(conn.Do("SMEMBERS", key))
	if err != nil {
		return uids
	}
	
	db, err := sql.Open("mysql", config.mysqldb_appdatasource)
	if err != nil {
		log.Info("error:", err)
		return uids
	}
	defer db.Close()
	
	if len(members) == 0 {
		ms, err := LoadGroupMember(db, gid)
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

func OpAddGroupMember(db *sql.DB, gid int64, uid int64, isOwner int) bool {
	if !AddGroupMember(db, gid, uid, isOwner) {
		return false
	}
	
	conn := redis_pool.Get()
	defer conn.Close()
	
	key := fmt.Sprintf("group_members_%d", gid)
	_, err := conn.Do("SADD", key, uid)
	if err != nil {
		log.Infoln(err)
	}
	
	key = fmt.Sprintf("user_groups_%d", uid)
	_, err = conn.Do("SADD", key, gid)
	if err != nil {
		log.Infoln(err)
	}
	
	return true
}

func OpRemoveGroupMember(db *sql.DB, gid int64, uid int64) bool {
	if !RemoveGroupMember(db, gid, uid) {
		return false
	}
	
	conn := redis_pool.Get()
	defer conn.Close()
	
	key := fmt.Sprintf("group_members_%d", gid)
	_, err := conn.Do("SREM", key, uid)
	if err != nil {
		log.Infoln(err)
	}
	
	key = fmt.Sprintf("user_groups_%d", uid)
	_, err = conn.Do("SREM", key, gid)
	if err != nil {
		log.Infoln(err)
	}
	
	return true
}

func AddGroupMember(db *sql.DB, group_id int64, uid int64, isOwner int) bool {
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

func RemoveGroupMember(db *sql.DB, group_id int64, uid int64) bool {	
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

func LoadGroupMember(db *sql.DB, group_id int64) ([]int64, error) {	
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

func CreateGroup(db *sql.DB, id int64, title string, desc string, isPrivate int, isAllowInvite int, owner int64, gouhao int) bool {	
	stmt, err := db.Prepare("INSERT INTO `group` (`id`, `title`, `desc`, `owner`, `gouhao`, `isPrivate`, `isAllowInvite`, `create_time`) VALUES (?, ?, ?, ?, ?, ?, ?, ?)")
	if err != nil {
		log.Info("error:", err)
		return false
	}
	
	defer stmt.Close()
	
	_, err = stmt.Exec(id, title, desc, owner, gouhao, isPrivate, isAllowInvite, time.Now().Unix())
	if err != nil {
		log.Info("error:", err)
		return false
	}
	
	return true
}

func DeleteGroup(db *sql.DB, id int64) bool {
	stmt, err := db.Prepare("UPDATE `group` SET isDeleted=1 WHERE id=?")
	if err != nil {
		log.Info("error:", err)
		return false
	}
	
	defer stmt.Close()
	
	_, err = stmt.Exec(id)
	if err != nil {
		log.Info("error:", err)
		return false
	}
	
	return true
}

func OpLoadAllGroup(db *sql.DB) {
	stmtIns, err := db.Prepare("select id, title, desc, isPrivate, isAllowInvite, owner, gouhao from `group` where isDeleted=0 and type=1")
	if err != nil {
		log.Info("error:", err)
		return
	}

	defer stmtIns.Close()
	
	conn := redis_pool.Get()
	defer conn.Close()
	
	rows, err := stmtIns.Query()
	for rows.Next() {
		var gid int64
		var title string
		var desc string
		var is_private int
		var is_allow_invite int
		var owner int64
		var gouhao int
		
		rows.Scan(&gid, &title, &desc, &is_private, &is_allow_invite, &owner, &gouhao)

		//建立群信息
		key := fmt.Sprintf("group_%d", gid)
		b, err := redis.Bool(conn.Do("EXISTS", key))
		if err != nil {
			continue
		}
		
		if b {
			continue
		}
		
		_, err = conn.Do("HMSET", key, "title", title, "desc", desc, "is_private", is_private, "is_allow_invite", is_allow_invite, "owner", owner, "gouhao", gouhao)
		if err != nil {
			log.Infoln(err)
			continue
		}
		
		//群成员
		//成员群关系
		members, err := LoadGroupMember(db, gid)
		if err != nil {
			log.Info("error:", err)
			continue
		}
		
		for _, uid := range members {
			key = fmt.Sprintf("group_members_%d", gid)
			_, err := conn.Do("SADD", key, uid)
			if err != nil {
				log.Infoln(err)
			}
			
			key = fmt.Sprintf("user_groups_%d", uid)
			_, err = conn.Do("SADD", key, gid)
			if err != nil {
				log.Infoln(err)
			}
		}
	}
	
}


func GenerateGroupUUID(uid int64) int64 {
	return int64(time.Now().UnixNano() / 1000) * 1000000 + uid % 1000000
}
