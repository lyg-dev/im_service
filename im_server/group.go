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
	mutex   sync.Mutex
	members common.IntSet
}

func NewGroup(gid int64, appid int64, members []int64) *Group {
	group := new(Group)
	group.appid = appid
	group.gid = gid
	group.super = false
	group.members = common.NewIntSet()
	for _, m := range members {
		group.members.Add(m)
	}
	return group
}

func NewSuperGroup(gid int64, appid int64, members []int64) *Group {
	group := new(Group)
	group.appid = appid
	group.gid = gid
	group.super = true
	group.members = common.NewIntSet()
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

func AddGroupMember(db *sql.DB, group_id int64, uid int64) bool {
	sql := fmt.Sprintf("INSERT INTO `group_members_0%d` ( `group_id`, `user_id`, `create_time`, `update_time`) select '%d', %d, %d, %d from dual where not exists(select * from group_members_0%d where group_id='%d' and user_id=%d)",
			group_id % 10, group_id, uid, time.Now().Unix(), time.Now().Unix(), group_id % 10, group_id, uid)
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

func RemoveGroupMember(db *sql.DB, group_id int64, uid int64) bool {
	sql := fmt.Sprintf("delete from group_members_0%d where group_id=? and user_id=?", group_id % 10);
	stmtIns, err := db.Prepare(sql)
	if err != nil {
		log.Info("error:", err)
		return false
	}
	defer stmtIns.Close()
	_, err = stmtIns.Exec(group_id, uid)
	if err != nil {
		log.Info("error:", err)
		return false
	}
	return true
}

func LoadAllGroup(db *sql.DB) (map[int64]*Group, error) {
	stmtIns, err := db.Prepare("select id from `group` where isDeleted=0 and type=1")
	if err != nil {
		log.Info("error:", err)
		return nil, nil
	}

	defer stmtIns.Close()
	groups := make(map[int64]*Group)
	rows, err := stmtIns.Query()
	for rows.Next() {
		var id int64
		rows.Scan(&id)
		members, err := LoadGroupMember(db, id)
		if err != nil {
			log.Info("error:", err)
			continue
		}

		group := NewSuperGroup(id, 1, members)
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
