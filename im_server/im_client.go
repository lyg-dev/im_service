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
	"encoding/json"
)
import "sync/atomic"
import log "github.com/golang/glog"
import "database/sql"
import _ "github.com/go-sql-driver/mysql"
import "github.com/garyburd/redigo/redis"

type IMClient struct {
	*Connection
}

func (client *IMClient) Login() {
	client.LoadOffline()
	client.LoadGroupOffline()
}

func (client *IMClient) Logout() {
	if client.uid > 0 {
		
	}
}

func (client *IMClient) LoadGroupOfflineMessage(gid int64) ([]*EMessage, error) {
	storage_pool := GetGroupStorageConnPool(gid)
	storage, err := storage_pool.Get()
	if err != nil {
		log.Error("connect storage err:", err)
		return nil, err
	}
	defer storage_pool.Release(storage)

	return storage.LoadGroupOfflineMessage(client.appid, gid, client.uid, client.device_ID)
}

func (client *IMClient) LoadGroupOffline() {
	if client.device_ID == 0 {
		return
	}

	gids := OpGetUserGroups(client.uid)
	for _, gid := range gids {
		messages, err := client.LoadGroupOfflineMessage(gid)
		if err != nil {
			log.Errorf("load group offline message err:%d %s", gid, err)
			continue
		}

		for _, emsg := range messages {
			client.owt <- emsg
		}
	}
}

func (client *IMClient) LoadOffline() {
	if client.device_ID == 0 {
		return
	}
	storage_pool := GetStorageConnPool(client.uid)
	storage, err := storage_pool.Get()
	if err != nil {
		log.Error("connect storage err:", err)
		return
	}
	defer storage_pool.Release(storage)

	messages, err := storage.LoadOfflineMessage(client.appid, client.uid, client.device_ID)
	if err != nil {
		log.Errorf("load offline message err:%d %s", client.uid, err)
		return
	}
	for _, emsg := range messages {
		client.owt <- emsg
	}
}

func (client *IMClient) HandleIMMessage(msg *IMMessage, seq int) {
	if client.uid == 0 {
		log.Warning("client has't been authenticated")
		return
	}

	if msg.sender != client.uid {
		log.Warningf("im message sender:%d client uid:%d\n", msg.sender, client.uid)
		return
	}
	
	//判断黑名单
	if OpIsUserBlack(msg.receiver, msg.sender) {
		client.wt <- &Message{cmd: MSG_ACK, body: &MessageACK{int32(seq)}}
		return
	}
	
	msg.timestamp = int32(time.Now().Unix())
	m := &Message{cmd: MSG_IM, version:DEFAULT_VERSION, body: msg}

	msgid, err := SaveMessage(client.appid, msg.receiver, client.device_ID, m)
	if err != nil {
		return
	}

	//保存到自己的消息队列，这样用户的其它登陆点也能接受到自己发出的消息
	SaveMessage(client.appid, msg.sender, client.device_ID, m)

	client.wt <- &Message{cmd: MSG_ACK, body: &MessageACK{int32(seq)}}

	atomic.AddInt64(&server_summary.in_message_count, 1)
	log.Infof("peer message sender:%d receiver:%d msgid:%d\n", msg.sender, msg.receiver, msgid)
}

func (client *IMClient) HandleGroupIMMessage(msg *IMMessage, seq int) {
	if client.uid == 0 {
		log.Warning("client has't been authenticated")
		return
	}

	msg.timestamp = int32(time.Now().Unix())
	m := &Message{cmd: MSG_GROUP_IM, version:DEFAULT_VERSION, body: msg}

	group := OpGetGroup(msg.receiver)
	if group == nil {
		log.Warning("can't find group:", msg.receiver)
		return
	}
	
	msgid, err := SaveGroupMessage(client.appid, msg.receiver, client.device_ID, m)
	if err != nil {
		return
	}
	
	client.wt <- &Message{cmd: MSG_ACK, body: &MessageACK{int32(seq)}}
	atomic.AddInt64(&server_summary.in_message_count, 1)
	log.Infof("group message sender:%d group id:%d msgid:%d", msg.sender, msg.receiver, msgid)
}

func (client *IMClient) HandleInputing(inputing *MessageInputing) {
	msg := &Message{cmd: MSG_INPUTING, body: inputing}
	client.SendMessage(inputing.receiver, msg)
	log.Infof("inputting sender:%d receiver:%d", inputing.sender, inputing.receiver)
}

func (client *IMClient) HandleACK(ack *MessageACK) {
	log.Info("ack:", ack)
	emsg := client.RemoveUnAckMessage(ack)
	if emsg == nil || emsg.msgid == 0 {
		return
	}

	msg := emsg.msg
	if msg != nil && msg.cmd == MSG_GROUP_IM {
		im := emsg.msg.body.(*IMMessage)
		group := OpGetGroup(im.receiver)
		if group != nil{
			client.DequeueGroupMessage(emsg.msgid, im.receiver)
		}
	} else {
		client.DequeueMessage(emsg.msgid)
	}

	if msg == nil {
		return
	}
}


func (client *IMClient) HandleSubsribe(msg *MessageSubscribeState) {
	if client.uid == 0 {
		return
	}

	//todo 获取在线状态
	for _, uid := range msg.uids {
		state := &MessageOnlineState{uid, 0}
		m := &Message{cmd: MSG_ONLINE_STATE, body: state}
		client.wt <- m
	}
}

func (client *IMClient) HandleRTMessage(msg *Message) {
	rt := msg.body.(*RTMessage)
	if rt.sender != client.uid {
		log.Warningf("rt message sender:%d client uid:%d\n", rt.sender, client.uid)
		return
	}
	
	m := &Message{cmd:MSG_RT, body:rt}
	client.SendMessage(rt.receiver, m)

	atomic.AddInt64(&server_summary.in_message_count, 1)
	log.Infof("realtime message sender:%d receiver:%d", rt.sender, rt.receiver)
}

func (client *IMClient) HandleTransmitUser(msg *IMMessage, seq int) {
	if client.uid == 0 {
		log.Warning("client has't been authenticated")
		return
	}

	if msg.sender != client.uid {
		log.Warningf("transmit message sender:%d client uid:%d\n", msg.sender, client.uid)
		return
	}
	
	//判断黑名单
	if OpIsUserBlack(msg.receiver, msg.sender) {
		client.wt <- &Message{cmd: MSG_ACK, body: &MessageACK{int32(seq)}}
		return
	}
	
	msg.timestamp = int32(time.Now().Unix())
	m := &Message{cmd: MSG_TRANSMIT_USER, version:DEFAULT_VERSION, body: msg}

	msgid, err := SaveMessage(client.appid, msg.receiver, client.device_ID, m)
	if err != nil {
		return
	}

	client.wt <- &Message{cmd: MSG_ACK, body: &MessageACK{int32(seq)}}

	atomic.AddInt64(&server_summary.in_message_count, 1)
	log.Infof("peer transmit message sender:%d receiver:%d msgid:%d\n", msg.sender, msg.receiver, msgid)
}

func (client *IMClient) HandleTransmitGroup(msg *IMMessage, seq int) {
	if client.uid == 0 {
		log.Warning("client has't been authenticated")
		return
	}

	msg.timestamp = int32(time.Now().Unix())
	m := &Message{cmd: MSG_TRANSMIT_GROUP, version:DEFAULT_VERSION, body: msg}

	msgid, err := SaveGroupMessage(client.appid, msg.receiver, client.device_ID, m)
	if err != nil {
		return
	}
	
	client.wt <- &Message{cmd: MSG_ACK, body: &MessageACK{int32(seq)}}
	atomic.AddInt64(&server_summary.in_message_count, 1)
	log.Infof("group message sender:%d group id:%d msgid:%d\n", msg.sender, msg.receiver, msgid)
}

func (client *IMClient) HandleMessage(msg *Message) {
	switch msg.cmd {
	case MSG_IM:
		client.HandleIMMessage(msg.body.(*IMMessage), msg.seq)
	case MSG_GROUP_IM:
		client.HandleGroupIMMessage(msg.body.(*IMMessage), msg.seq)
	case MSG_ACK:
		client.HandleACK(msg.body.(*MessageACK))
	case MSG_INPUTING:
		client.HandleInputing(msg.body.(*MessageInputing))
	case MSG_SUBSCRIBE_ONLINE_STATE:
		client.HandleSubsribe(msg.body.(*MessageSubscribeState))
	case MSG_RT:
		client.HandleRTMessage(msg)
	case MSG_TRANSMIT_USER:
		client.HandleTransmitUser(msg.body.(*IMMessage), msg.seq)
	case MSG_TRANSMIT_GROUP:
		client.HandleTransmitGroup(msg.body.(*IMMessage), msg.seq)
	case MSG_CONTACT_INVITE:
		client.HandleContactInvite(msg.body.(*ContactInvite))
	case MSG_CONTACT_ACCEPT:
		client.HandleContactAccept(msg.body.(*ContactAccept))
	case MSG_CONTACT_REFUSE:
		client.HandleContactRefuse(msg.body.(*ContactRefuse))
	case MSG_CONTACT_DEL:
		client.HandleContactDel(msg.body.(*ContactDel))
	case MSG_CONTACT_BLACK:
		client.HandleContactBlack(msg.body.(*ContactBlack))
	case MSG_CONTACT_UNBLACK:
		client.HandleContactUnBlack(msg.body.(*ContactUnBlack))
	case MSG_GROUP_CREATE:
		client.handlerGroupCreate(msg.body.(*GroupCreate))
	case MSG_GROUP_SELF_JOIN:
		client.handlerGroupSelfJoin(msg.body.(*GroupSelfJoin))
	case MSG_GROUP_INVITE_JOIN:
		client.handlerGroupInviteJoin(msg.body.(*GroupInviteJoin))
	case MSG_GROUP_REMOVE:
		client.handlerGroupRemove(msg.body.(*GroupRemove))
	case MSG_GROUP_QUIT:
		client.handlerGroupQuit(msg.body.(*GroupQuit))
	case MSG_GROUP_DEL:
		client.handlerGroupDel(msg.body.(*GroupDel))
	}
}

func (client *IMClient) handlerGroupDel(groupDel *GroupDel) {
	group := OpGetGroup(groupDel.gid)
	
	if group == nil {
		msg := &Message{cmd: MSG_GROUP_DEL_RESP, version:DEFAULT_VERSION, body: &SimpleResp{1}}
		client.wt <- msg
			
		return
	}
	
	if group.owner != client.uid {
		msg := &Message{cmd: MSG_GROUP_DEL_RESP, version:DEFAULT_VERSION, body: &SimpleResp{2}}
		client.wt <- msg
			
		return
	}
	
	db, err := sql.Open("mysql", config.mysqldb_appdatasource)
	if err != nil {
		log.Info("error:", err)
		return
	}
	defer db.Close()
	
	members := OpGetGroupMembers(groupDel.gid)
	
	for _, member := range members {
		OpRemoveGroupMember(db, groupDel.gid, member)
		
		//构造一条透传发送群解散透传
		obj := make(map[string]interface{})
		obj["cmd"] = CMD_CALLBACK_GROUP_DEL
		obj["from"] = client.uid
		obj["to"] = group.gid
		obj["msg"] = ""
		
		msg := &IMMessage{}
		msg.sender = client.uid
		msg.receiver = member
		msg.timestamp = int32(time.Now().Unix())
		content, err := json.Marshal(obj)
		if err != nil {
			return
		}
		msg.content = string(content)
		m := &Message{cmd: MSG_TRANSMIT_USER, version:DEFAULT_VERSION, body: msg}
	
		SaveMessage(client.appid, msg.receiver, client.device_ID, m)
	}
	
	OpDelGroup(db, groupDel.gid)
	
	msg := &Message{cmd: MSG_GROUP_DEL_RESP, version:DEFAULT_VERSION, body: &SimpleResp{0}}
	client.wt <- msg
}

func (client *IMClient) handlerGroupQuit(groupQuit *GroupQuit) {
	group := OpGetGroup(groupQuit.gid)
	
	if group == nil {
		msg := &Message{cmd: MSG_GROUP_QUIT_RESP, version:DEFAULT_VERSION, body: &SimpleResp{1}}
		client.wt <- msg
			
		return
	}
	
	if group.owner == client.uid {
		msg := &Message{cmd: MSG_GROUP_QUIT_RESP, version:DEFAULT_VERSION, body: &SimpleResp{2}}
		client.wt <- msg
			
		return
	}
	
	db, err := sql.Open("mysql", config.mysqldb_appdatasource)
	if err != nil {
		log.Info("error:", err)
		return
	}
	defer db.Close()
	
	if !RemoveGroupMember(db, group.gid, client.uid) {
		msg := &Message{cmd: MSG_GROUP_QUIT_RESP, version:DEFAULT_VERSION, body: &SimpleResp{3}}
		client.wt <- msg
			
		return
	}
	
	msg := &Message{cmd: MSG_GROUP_QUIT_RESP, version:DEFAULT_VERSION, body: &SimpleResp{0}}
	client.wt <- msg
}

func (client *IMClient) handlerGroupRemove(groupRemove *GroupRemove) {
	group := OpGetGroup(groupRemove.gid)
	
	if group == nil {
		msg := &Message{cmd: MSG_GROUP_REMOVE_RESP, version:DEFAULT_VERSION, body: &SimpleResp{1}}
		client.wt <- msg
			
		return
	}
	
	if client.uid != group.owner {
		msg := &Message{cmd: MSG_GROUP_REMOVE_RESP, version:DEFAULT_VERSION, body: &SimpleResp{2}}
		client.wt <- msg
			
		return
	}
	
	if group.owner == groupRemove.uid {
		msg := &Message{cmd: MSG_GROUP_REMOVE_RESP, version:DEFAULT_VERSION, body: &SimpleResp{3}}
		client.wt <- msg
			
		return
	}
	
	db, err := sql.Open("mysql", config.mysqldb_appdatasource)
	if err != nil {
		log.Info("error:", err)
		return
	}
	defer db.Close()
	
	if !OpRemoveGroupMember(db, group.gid, groupRemove.uid) {
		msg := &Message{cmd: MSG_GROUP_REMOVE_RESP, version:DEFAULT_VERSION, body: &SimpleResp{4}}
		client.wt <- msg
			
		return
	}
	
	//构造一条透传发送被移除透传
	obj := make(map[string]interface{})
	obj["cmd"] = CMD_CALLBACK_GROUP_REMOVE
	obj["from"] = client.uid
	obj["to"] = group.gid
	obj["msg"] = ""
	
	msg := &IMMessage{}
	msg.sender = client.uid
	msg.receiver = groupRemove.uid
	msg.timestamp = int32(time.Now().Unix())
	content, err := json.Marshal(obj)
	if err != nil {
		return
	}
	msg.content = string(content)
	m := &Message{cmd: MSG_TRANSMIT_USER, version:DEFAULT_VERSION, body: msg}

	SaveMessage(client.appid, msg.receiver, client.device_ID, m)
	
	msgResp := &Message{cmd: MSG_GROUP_REMOVE_RESP, version:DEFAULT_VERSION, body: &SimpleResp{0}}
	client.wt <- msgResp
}

func (client *IMClient) handlerGroupInviteJoin(groupInviteJoin *GroupInviteJoin) {
	group := OpGetGroup(groupInviteJoin.gid)
	
	if group == nil {
		msg := &Message{cmd: MSG_GROUP_INVITE_JOIN_RESP, version:DEFAULT_VERSION, body: &SimpleResp{1}}
		client.wt <- msg
			
		return
	}
	
	if group.owner != client.uid && group.is_allow_invite == 0{
		msg := &Message{cmd: MSG_GROUP_INVITE_JOIN_RESP, version:DEFAULT_VERSION, body: &SimpleResp{2}}
		client.wt <- msg
			
		return
	}
	
	if !OpIsGroupMember(group.gid, client.uid) {
		msg := &Message{cmd: MSG_GROUP_INVITE_JOIN_RESP, version:DEFAULT_VERSION, body: &SimpleResp{3}}
		client.wt <- msg
			
		return
	}
	
	if (OpGetGroupMemberNumber(group.gid)+len(groupInviteJoin.members)) > 500 {
		msg := &Message{cmd: MSG_GROUP_INVITE_JOIN_RESP, version:DEFAULT_VERSION, body: &SimpleResp{4}}
		client.wt <- msg
			
		return
	} 
	
	db, err := sql.Open("mysql", config.mysqldb_appdatasource)
	if err != nil {
		log.Info("error:", err)
		return
	}
	defer db.Close()
	
	for _, member := range groupInviteJoin.members {
		if OpIsGroupMember(group.gid, member) {
			continue
		}
		
		if !OpAddGroupMember(db, group.gid, member, 0) {
			continue
		}
		
		//构造一条透传发送被拉入群
		obj := make(map[string]interface{})
		obj["cmd"] = CMD_CALLBACK_GROUP_JOIN
		obj["from"] = client.uid
		obj["to"] = group.gid
		obj["msg"] = ""
		
		msg := &IMMessage{}
		msg.sender = client.uid
		msg.receiver = member
		msg.timestamp = int32(time.Now().Unix())
		content, err := json.Marshal(obj)
		if err != nil {
			return
		}
		msg.content = string(content)
		m := &Message{cmd: MSG_TRANSMIT_USER, version:DEFAULT_VERSION, body: msg}
	
		SaveMessage(client.appid, msg.receiver, client.device_ID, m)
	}
	
	msg := &Message{cmd: MSG_GROUP_INVITE_JOIN_RESP, version:DEFAULT_VERSION, body: &SimpleResp{0}}
	client.wt <- msg
}

func (client *IMClient) handlerGroupSelfJoin(groupSelfJoin *GroupSelfJoin) {
	group := OpGetGroup(groupSelfJoin.gid)
	
	if group == nil {
		msg := &Message{cmd: MSG_GROUP_SELF_JOIN_RESP, version:DEFAULT_VERSION, body: &SimpleResp{1}}
		client.wt <- msg
			
		return
	}
	
	if group.owner == client.uid {
		msg := &Message{cmd: MSG_GROUP_SELF_JOIN_RESP, version:DEFAULT_VERSION, body: &SimpleResp{2}}
		client.wt <- msg
			
		return
	}
	
	if group.is_private != 0 {
		msg := &Message{cmd: MSG_GROUP_SELF_JOIN_RESP, version:DEFAULT_VERSION, body: &SimpleResp{3}}
		client.wt <- msg
			
		return
	}
	
	if OpGetGroupMemberNumber(groupSelfJoin.gid) >= 500 {
		msg := &Message{cmd: MSG_GROUP_SELF_JOIN_RESP, version:DEFAULT_VERSION, body: &SimpleResp{4}}
		client.wt <- msg
			
		return
	}
	
	db, err := sql.Open("mysql", config.mysqldb_appdatasource)
	if err != nil {
		log.Info("error:", err)
		return
	}
	defer db.Close()
	
	if OpIsGroupMember(group.gid, client.uid) {
		if !OpAddGroupMember(db, group.gid, client.uid, 0) {
			msg := &Message{cmd: MSG_GROUP_SELF_JOIN_RESP, version:DEFAULT_VERSION, body: &SimpleResp{5}}
			client.wt <- msg
				
			return
		}
	}
	
	msg := &Message{cmd: MSG_GROUP_SELF_JOIN_RESP, version:DEFAULT_VERSION, body: &SimpleResp{0}}
	client.wt <- msg
}

func (client *IMClient) handlerGroupCreate(groupCreate *GroupCreate) {
	if len(groupCreate.members) + 1 > 500 {
		msg := &Message{cmd: MSG_GROUP_CREATE_RESP, version:DEFAULT_VERSION, body: &GroupCreateResp{1, 0}}
		client.wt <- msg
			
		return
	}
	
	db, err := sql.Open("mysql", config.mysqldb_appdatasource)
	if err != nil {
		log.Info("error:", err)
		return
	}
	defer db.Close()
	
	conn := redis_pool.Get()
	defer conn.Close()
	
	gouhao, err := redis.Int(conn.Do("SPOP", "user_app_group_gouhao_set"))
	if err != nil {
		log.Info("get group gouhao err", err)
	}
	
	gid := GenerateGroupUUID(client.uid)

	
	if !OpCreateGroup(db, gid, groupCreate.title, groupCreate.desc, int(groupCreate.is_private), int(groupCreate.is_allow_invite), client.uid, gouhao) {
		msg := &Message{cmd: MSG_GROUP_CREATE_RESP, version:DEFAULT_VERSION, body: &GroupCreateResp{2, 0}}
		client.wt <- msg
			
		return
	}
	
	OpAddGroupMember(db, gid, client.uid, 1)
	for _, member := range groupCreate.members {
		if member == client.uid {
			continue
		}
		
		if OpAddGroupMember(db, gid, member, 0) {
			//构造一条透传发送被拉入群
			obj := make(map[string]interface{})
			obj["cmd"] = CMD_CALLBACK_GROUP_JOIN
			obj["from"] = client.uid
			obj["to"] = gid
			obj["msg"] = ""
			
			msg := &IMMessage{}
			msg.sender = client.uid
			msg.receiver = member
			msg.timestamp = int32(time.Now().Unix())
			content, err := json.Marshal(obj)
			if err != nil {
				return
			}
			msg.content = string(content)
			m := &Message{cmd: MSG_TRANSMIT_USER, version:DEFAULT_VERSION, body: msg}
		
			SaveMessage(client.appid, msg.receiver, client.device_ID, m)
		}
	}
	
	msg := &Message{cmd: MSG_GROUP_CREATE_RESP, version:DEFAULT_VERSION, body: &GroupCreateResp{0, gid}}
	client.wt <- msg
}

func (client *IMClient) HandleContactBlack(contactBlack *ContactBlack) {
	if contactBlack.sender == contactBlack.receiver {
		log.Infof("contact black sender: %d, receiver: %d", contactBlack.sender, contactBlack.receiver)
		
		msg := &Message{cmd: MSG_CONTACT_BLACK_RESP, version:DEFAULT_VERSION, body: &ContactBlackResp{1, contactBlack.sender, contactBlack.receiver}}
		client.wt <- msg
			
		return
	}
	
	db, err := sql.Open("mysql", config.mysqldb_appdatasource)
	if err != nil {
		log.Info("error:", err)
		return
	}
	defer db.Close()
	
	//拉入黑名单
	if !OpAddUserBlack(db, contactBlack.sender, contactBlack.receiver) {
		msg := &Message{cmd: MSG_CONTACT_BLACK_RESP, version:DEFAULT_VERSION, body: &ContactBlackResp{3, contactBlack.sender, contactBlack.receiver}}
		client.wt <- msg
			
		return
	}
	
	msg := &Message{cmd: MSG_CONTACT_BLACK_RESP, version:DEFAULT_VERSION, body: &ContactBlackResp{0, contactBlack.sender, contactBlack.receiver}}
	client.wt <- msg
}

func (client *IMClient) HandleContactUnBlack(contactUnBlack *ContactUnBlack) {
	if contactUnBlack.sender == contactUnBlack.receiver {
		log.Infof("contact unblack sender: %d, receiver: %d", contactUnBlack.sender, contactUnBlack.receiver)
		
		msg := &Message{cmd: MSG_CONTACT_UNBLACK_RESP, version:DEFAULT_VERSION, body: &ContactUnBlackResp{1, contactUnBlack.sender, contactUnBlack.receiver}}
		client.wt <- msg
			
		return
	}
	
	db, err := sql.Open("mysql", config.mysqldb_appdatasource)
	if err != nil {
		log.Info("error:", err)
		return
	}
	defer db.Close()
	
	//解除黑名单
	if !OpRemoveUserBlack(db, contactUnBlack.sender, contactUnBlack.receiver) {
		msg := &Message{cmd: MSG_CONTACT_UNBLACK_RESP, version:DEFAULT_VERSION, body: &ContactUnBlackResp{2, contactUnBlack.sender, contactUnBlack.receiver}}
		client.wt <- msg
			
		return
	}
	
	msg := &Message{cmd: MSG_CONTACT_UNBLACK_RESP, version:DEFAULT_VERSION, body: &ContactUnBlackResp{0, contactUnBlack.sender, contactUnBlack.receiver}}
	client.wt <- msg
}

//申请加好友
func (client *IMClient) HandleContactInvite(contactInvite *ContactInvite) {
	if contactInvite.sender == contactInvite.receiver {
		log.Infof("contact invite sender: %d, receiver: %d", contactInvite.sender, contactInvite.receiver)
		
		msg := &Message{cmd: MSG_CONTACT_INVITE_RESP, version:DEFAULT_VERSION, body: &ContactInviteResp{1, contactInvite.sender, contactInvite.receiver}}
		client.wt <- msg
			
		return
	}
	
	if OpIsUserFriend(contactInvite.sender, contactInvite.receiver) {
		
		msg := &Message{cmd: MSG_CONTACT_INVITE_RESP, version:DEFAULT_VERSION, body: &ContactInviteResp{2, contactInvite.sender, contactInvite.receiver}}
		client.wt <- msg
		
		return
	}
	
	if OpIsUserBlack(contactInvite.receiver, contactInvite.sender) {
		msg := &Message{cmd: MSG_CONTACT_INVITE_RESP, version:DEFAULT_VERSION, body: &ContactInviteResp{3, contactInvite.sender, contactInvite.receiver}}
		client.wt <- msg
		
		return
	}
	
	db, err := sql.Open("mysql", config.mysqldb_appdatasource)
	if err != nil {
		log.Info("error:", err)
		return
	}
	defer db.Close()
	
	if !OpHasUserInfoById(db, contactInvite.receiver) {
		
		msg := &Message{cmd: MSG_CONTACT_INVITE_RESP, version:DEFAULT_VERSION, body: &ContactInviteResp{4, contactInvite.sender, contactInvite.receiver}}
		client.wt <- msg
		
		return;
	}
	
	//如果在黑名单中，自动解除黑名单
	OpRemoveUserBlack(db, contactInvite.sender, contactInvite.receiver)
	
	//构造一条透传发送好友申请
	obj := make(map[string]interface{})
	obj["cmd"] = CMD_CALLBACK_FRIEND_INVITE
	obj["from"] = contactInvite.sender
	obj["to"] = contactInvite.receiver
	obj["msg"] = contactInvite.reason
	
	msg := &IMMessage{}
	msg.sender = contactInvite.sender
	msg.receiver = contactInvite.receiver
	msg.timestamp = int32(time.Now().Unix())
	content, err := json.Marshal(obj)
	if err != nil {
		return
	}
	msg.content = string(content)
	m := &Message{cmd: MSG_TRANSMIT_USER, version:DEFAULT_VERSION, body: msg}

	SaveMessage(client.appid, msg.receiver, client.device_ID, m)
	
	respMsg := &Message{cmd: MSG_CONTACT_INVITE_RESP, version:DEFAULT_VERSION, body: &ContactInviteResp{0, contactInvite.sender, contactInvite.receiver}}
	client.wt <- respMsg
}

func (client *IMClient) HandleContactAccept(contactAccept *ContactAccept) {
	if contactAccept.sender == contactAccept.receiver {
		log.Infof("contact accept sender: %d, receiver: %d", contactAccept.sender, contactAccept.receiver)
		
		msg := &Message{cmd: MSG_CONTACT_ACCEPT_RESP, version:DEFAULT_VERSION, body: &ContactAcceptResp{1, contactAccept.sender, contactAccept.receiver}}
		client.wt <- msg
			
		return
	}
	
	if OpIsUserFriend(contactAccept.sender, contactAccept.receiver) {
		
		msg := &Message{cmd: MSG_CONTACT_ACCEPT_RESP, version:DEFAULT_VERSION, body: &ContactAcceptResp{2, contactAccept.sender, contactAccept.receiver}}
		client.wt <- msg
		
		return
	}
	
	db, err := sql.Open("mysql", config.mysqldb_appdatasource)
	if err != nil {
		log.Info("error:", err)
		return
	}
	defer db.Close()
	
	if !OpHasUserInfoById(db, contactAccept.receiver) {
		
		msg := &Message{cmd: MSG_CONTACT_ACCEPT_RESP, version:DEFAULT_VERSION, body: &ContactAcceptResp{4, contactAccept.sender, contactAccept.receiver}}
		client.wt <- msg
		
		return;
	}
	
	//如果在黑名单中，自动解除黑名单
	if OpIsUserBlack(contactAccept.sender, contactAccept.receiver) {
		OpRemoveUserBlack(db, contactAccept.sender, contactAccept.receiver)
	}
	
	if OpIsUserBlack(contactAccept.receiver, contactAccept.sender) {
		OpRemoveUserBlack(db, contactAccept.receiver, contactAccept.sender)
	}
	
	//建立好友关系
	if !OpAddUserFriend(db, contactAccept.sender, contactAccept.receiver) {
		return
	}
	
	//构造一条透传发送加好友申请通过通知
	obj := make(map[string]interface{})
	obj["cmd"] = CMD_CALLBACK_FRIEND_ACCEPT
	obj["from"] = contactAccept.sender
	obj["to"] = contactAccept.receiver
	obj["msg"] = ""
	
	msg := &IMMessage{}
	msg.sender = contactAccept.sender
	msg.receiver = contactAccept.receiver
	msg.timestamp = int32(time.Now().Unix())
	content, err := json.Marshal(obj)
	if err != nil {
		return
	}
	msg.content = string(content)
	m := &Message{cmd: MSG_TRANSMIT_USER, version:DEFAULT_VERSION, body: msg}

	SaveMessage(client.appid, msg.receiver, client.device_ID, m)
	
	//构造一条透传发送添加好友成功回调
	obj = make(map[string]interface{})
	obj["cmd"] = CMD_CALLBACK_FRIEND_ADD
	obj["from"] = contactAccept.sender
	obj["to"] = contactAccept.receiver
	obj["msg"] = ""
	
	msg = &IMMessage{}
	msg.sender = contactAccept.sender
	msg.receiver = contactAccept.receiver
	msg.timestamp = int32(time.Now().Unix())
	content, err = json.Marshal(obj)
	if err != nil {
		return
	}
	msg.content = string(content)
	m = &Message{cmd: MSG_TRANSMIT_USER, version:DEFAULT_VERSION, body: msg}

	SaveMessage(client.appid, msg.receiver, client.device_ID, m)
	
	respMsg := &Message{cmd: MSG_CONTACT_ACCEPT_RESP, version:DEFAULT_VERSION, body: &ContactAcceptResp{0, contactAccept.sender, contactAccept.receiver}}
	client.wt <- respMsg
}

func (client *IMClient) HandleContactRefuse(contactRefuse *ContactRefuse) {
	if contactRefuse.sender == contactRefuse.receiver {
		log.Infof("contact refuse sender: %d, receiver: %d", contactRefuse.sender, contactRefuse.receiver)
		
		msg := &Message{cmd: MSG_CONTACT_REFUSE_RESP, version:DEFAULT_VERSION, body: &ContactRefuseResp{1, contactRefuse.sender, contactRefuse.receiver}}
		client.wt <- msg
			
		return
	}
	
	if OpIsUserFriend(contactRefuse.sender, contactRefuse.receiver) {
		
		msg := &Message{cmd: MSG_CONTACT_REFUSE_RESP, version:DEFAULT_VERSION, body: &ContactRefuseResp{2, contactRefuse.sender, contactRefuse.receiver}}
		client.wt <- msg
		
		return
	}
	
	if OpIsUserBlack(contactRefuse.receiver, contactRefuse.sender) {
		msg := &Message{cmd: MSG_CONTACT_REFUSE_RESP, version:DEFAULT_VERSION, body: &ContactRefuseResp{3, contactRefuse.sender, contactRefuse.receiver}}
		client.wt <- msg
		
		return
	}
	
	//构造一条透传发送好友申请被拒绝通知
	obj := make(map[string]interface{})
	obj["cmd"] = CMD_CALLBACK_FRIEND_REFUSE
	obj["from"] = contactRefuse.sender
	obj["to"] = contactRefuse.receiver
	
	msg := &IMMessage{}
	msg.sender = contactRefuse.sender
	msg.receiver = contactRefuse.receiver
	msg.timestamp = int32(time.Now().Unix())
	content, err := json.Marshal(obj)
	if err != nil {
		return
	}
	msg.content = string(content)
	m := &Message{cmd: MSG_TRANSMIT_USER, version:DEFAULT_VERSION, body: msg}

	SaveMessage(client.appid, msg.receiver, client.device_ID, m)
	
	respMsg := &Message{cmd: MSG_CONTACT_REFUSE_RESP, version:DEFAULT_VERSION, body: &ContactRefuseResp{0, contactRefuse.sender, contactRefuse.receiver}}
	client.wt <- respMsg
}

func (client *IMClient) HandleContactDel(contactDel *ContactDel) {
	if contactDel.sender == contactDel.receiver {
		log.Infof("contact del sender: %d, receiver: %d", contactDel.sender, contactDel.receiver)
		
		msg := &Message{cmd: MSG_CONTACT_DEL_RESP, version:DEFAULT_VERSION, body: &ContactDelResp{1, contactDel.sender, contactDel.receiver}}
		client.wt <- msg
			
		return
	}
	
	if !OpIsUserFriend(contactDel.sender, contactDel.receiver) {
		
		msg := &Message{cmd: MSG_CONTACT_DEL_RESP, version:DEFAULT_VERSION, body: &ContactDelResp{2, contactDel.sender, contactDel.receiver}}
		client.wt <- msg
		
		return
	}
	
	db, err := sql.Open("mysql", config.mysqldb_appdatasource)
	if err != nil {
		log.Info("error:", err)
		return
	}
	defer db.Close()
	
	//删除好友关系
	if !OpRemoveUserFriend(db, contactDel.sender, contactDel.receiver) {
		return
	}
	
	//构造一条透传发送加好友删除通知
	obj := make(map[string]interface{})
	obj["cmd"] = CMD_CALLBACK_FRIEND_DEL
	obj["from"] = contactDel.sender
	obj["to"] = contactDel.receiver
	obj["msg"] = ""
	
	msg := &IMMessage{}
	msg.sender = contactDel.sender
	msg.receiver = contactDel.receiver
	msg.timestamp = int32(time.Now().Unix())
	content, err := json.Marshal(obj)
	if err != nil {
		return
	}
	msg.content = string(content)
	m := &Message{cmd: MSG_TRANSMIT_USER, version:DEFAULT_VERSION, body: msg}

	SaveMessage(client.appid, msg.receiver, client.device_ID, m)
	
	respMsg := &Message{cmd: MSG_CONTACT_DEL_RESP, version:DEFAULT_VERSION, body: &ContactDelResp{0, contactDel.sender, contactDel.receiver}}
	client.wt <- respMsg
}

func (client *IMClient) DequeueGroupMessage(msgid int64, gid int64) {
	storage_pool := GetGroupStorageConnPool(gid)
	storage, err := storage_pool.Get()
	if err != nil {
		log.Error("connect storage err:", err)
		return
	}
	defer storage_pool.Release(storage)

	dq := &DQGroupMessage{msgid:msgid, appid:client.appid, receiver:client.uid, gid:gid, device_id:client.device_ID}
	err = storage.DequeueGroupMessage(dq)
	if err != nil {
		log.Error("dequeue message err:", err)
	}
}

func (client *IMClient) DequeueMessage(msgid int64) {
	if client.device_ID == 0 {
		return
	}
	
	storage_pool := GetStorageConnPool(client.uid)
	storage, err := storage_pool.Get()
	if err != nil {
		log.Error("connect storage err:", err)
		return
	}
	defer storage_pool.Release(storage)

	dq := &DQMessage{msgid:msgid, appid:client.appid, receiver:client.uid, device_id:client.device_ID}
	err = storage.DequeueMessage(dq)
	if err != nil {
		log.Error("dequeue message err:", err)
	}
}

