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
	"sync"
	"fmt"
)
import "database/sql"
import _ "github.com/go-sql-driver/mysql"
import log "github.com/golang/glog"
import "im_service/common"

type Group struct {
	gid     int64
	appid   int64
	super   bool //超大群
	is_private bool
	is_allow_invite bool
	title	string
	desc	string
	owner int64
	mutex   sync.Mutex
	members common.IntSet
}

func NewGroup(gid int64, appid int64, members []int64, is_private bool, is_allow_invite bool, title string, desc string, owner int64) *Group {
	group := new(Group)
	group.appid = appid
	group.gid = gid
	group.super = false
	group.is_private = is_private
	group.is_allow_invite = is_allow_invite
	group.title = title
	group.desc = desc
	group.owner = owner
	group.members = common.NewIntSet()
	group.members.Add(group.owner)
	for _, m := range members {
		group.members.Add(m)
	}
	return group
}

func NewSuperGroup(gid int64, appid int64, members []int64, is_private bool, is_allow_invite bool, title string, desc string, owner int64) *Group {
	group := new(Group)
	group.appid = appid
	group.gid = gid
	group.super = true
	group.is_private = is_private
	group.is_allow_invite = is_allow_invite
	group.title = title
	group.desc = desc
	group.owner = owner
	group.members = common.NewIntSet()
	group.members.Add(group.owner)
	for _, m := range members {
		group.members.Add(m)
	}
	return group
}


func (group *Group) Members() common.IntSet {
	return group.members
}

//修改成员，在副本修改，避免读取时的lock
func (group *Group) AddMember(uid int64) {
	group.mutex.Lock()
	defer group.mutex.Unlock()
	members := group.members.Clone()
	members.Add(uid)
	group.members = members
}

func (group *Group) RemoveMember(uid int64) {
	group.mutex.Lock()
	defer group.mutex.Unlock()
	members := group.members.Clone()
	members.Remove(uid)
	group.members = members
}

func (group *Group) IsMember(uid int64) bool {
	_, ok := group.members[uid]
	return ok
}

func (group *Group) IsEmpty() bool {
	return len(group.members) == 0
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

func LoadGroupById(db *sql.DB, id int64) (string, string, bool, bool, int64, error) {
	stmtIns, err := db.Prepare("select `title`, `desc`, `owner`, `isPrivate`, `isAllowInvite` from `group` where id=?")
	if err != nil {
		log.Info("error:", err)
		return "", "", false, false, 0, err
	}

	defer stmtIns.Close()
	
	var title, desc string
	var owner int64
	var isPrivate, isAllowInvite int
	err = stmtIns.QueryRow(id).Scan(&title, &desc, &owner, &isPrivate, &isAllowInvite)
	if err != nil {
		log.Info("error:", err)
		return "", "", false, false, 0, err
	}
	
	isPri := true
	isAllow := true
	
	if isPrivate == 0 {
		isPri = false
	}
	
	if isAllowInvite == 0 {
		isAllow = false
	}
	
	return title, desc, isPri, isAllow, owner, nil
}

func LoadAllGroup(db *sql.DB) (map[int64]*Group, error) {
	stmtIns, err := db.Prepare("select `id`, `title`, `desc`, `owner`, `isPrivate`, `isAllowInvite` from `group` where isDeleted=0 and type=1")
	if err != nil {
		log.Info("error:", err)
		return nil, nil
	}

	defer stmtIns.Close()
	groups := make(map[int64]*Group)
	rows, err := stmtIns.Query()
	for rows.Next() {
		var id int64
		var owner int64
		var title string
		var desc string
		var isPrivate int
		var isAllowInvite int
		rows.Scan(&id, &title, &desc, &owner, &isPrivate, &isAllowInvite)
		members, err := LoadGroupMember(db, id)
		if err != nil {
			log.Info("error:", err)
			continue
		}

		isPri := true
		isAllow := true
		
		if isPrivate == 0 {
			isPri = false
		}
		
		if isAllowInvite == 0 {
			isAllow = false
		}
		
		group := NewGroup(id, 1, members, isPri, isAllow, title, desc, owner)
		groups[group.gid] = group
	}
	return groups, nil
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

func CreateGroup(db *sql.DB, id int64, title string, desc string, isPrivate int32, isAllowInvite int32, owner int64, gouhao int) bool {	
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

func GenerateGroupUUID(uid int64) int64 {
	return int64(time.Now().UnixNano() / 1000) * 1000000 + uid % 1000000
}
