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

import "sync"
import "encoding/json"
import log "github.com/golang/glog"
import ots2 "github.com/GiterLab/goots"
//import "github.com/GiterLab/goots/log"
import . "github.com/GiterLab/goots/otstype"

type GroupStorage struct {
	ots2_client *ots2.OTSClient
	mutex sync.Mutex
}

func NewGroupStorage(ots2_client *ots2.OTSClient) *GroupStorage {
	storage := &GroupStorage{}
	storage.ots2_client = ots2_client
	return storage
}

func (storage *GroupStorage) saveMessage(msg *Message) int64 {
	m := msg.body.(*IMMessage)
	
	msgid, err := iw.NextId()
	if err != nil {
		log.Fatalln(err)
	}

	m.msgid = msgid
	
	primaryKey := &OTSPrimaryKey{
		"gid" : m.receiver,
		"msgid" : m.msgid,
	}
	
	bs, err := json.Marshal(*m)
	if err != nil {
		log.Infoln(err)
		return 0
	}
	
	attributeColumns := &OTSAttribute{
		"cmd" : msg.cmd,
		"seq" : msg.seq,
		"version" : msg.version,
		"body" : string(bs),
	}
	
	condition := OTSCondition_EXPECT_NOT_EXIST
	storage.ots2_client.PutRow("msg_group", condition, primaryKey, attributeColumns)
	
	return msgid
}

func (storage *GroupStorage) SaveGroupMessage(appid int64, gid int64, device_id int64, msg *Message) int64 {
	storage.mutex.Lock()
	defer storage.mutex.Unlock()

	msgid := storage.saveMessage(msg)

	storage.setLastGroupMessageID(appid, gid, msgid)
	return msgid
}

func (storage *GroupStorage) setLastGroupMessageID(appid int64, gid int64, msgid int64) {
	primaryKey := &OTSPrimaryKey{
		"gid" : gid,
	}
	
	attributeColumns := &OTSAttribute{
		"msgid" : msgid,
	}
	
	condition := OTSCondition_IGNORE
	storage.ots2_client.PutRow("msg_group_last_id", condition, primaryKey, attributeColumns)
}

func (storage *GroupStorage) SetLastGroupMessageID(appid int64, gid int64, msgid int64) {
	storage.mutex.Lock()
	defer storage.mutex.Unlock()
	
	storage.setLastGroupMessageID(appid, gid, msgid)
}

func (storage *GroupStorage) getLastGroupMessageID(appid int64, gid int64) (int64, error) {
	primaryKey := &OTSPrimaryKey{
		"gid" : gid,
	}
	
	columnsToGet := &OTSColumnsToGet{
		"msgid",
	}
	
	get_row_response, err := storage.ots2_client.GetRow("msg_group_last_id", primaryKey, columnsToGet)
	if err != nil {
		return 0, err
	}
	
	if get_row_response.Row != nil {
		if attributeColumns := get_row_response.Row.GetAttributeColumns(); attributeColumns != nil {
			msgid := attributeColumns.Get("msgid").(int64)
			return msgid, nil
		}
	}
	
	return 0, nil
}

func (storage *GroupStorage) GetLastGroupMessageID(appid int64, gid int64) (int64, error) {
	storage.mutex.Lock()
	defer storage.mutex.Unlock()
	
	return storage.getLastGroupMessageID(appid, gid)
}

func (storage *GroupStorage) setLastGroupReceivedID(appid int64, gid int64, uid int64, did int64, msgid int64) {
	primaryKey := &OTSPrimaryKey{
		"gid" : gid,
		"uid" : uid,
		"deviceid" : did,
	}
	
	attributeColumns := &OTSAttribute{
		"msgid" : msgid,
	}
	
	condition := OTSCondition_IGNORE
	storage.ots2_client.PutRow("msg_group_user_last_recv_id", condition, primaryKey, attributeColumns)
}

func (storage *GroupStorage) SetLastGroupReceivedID(appid int64, gid int64, uid int64, device_id int64, msgid int64) {
	storage.mutex.Lock()
	defer storage.mutex.Unlock()
	
	storage.setLastGroupReceivedID(appid, gid, uid, device_id, msgid)
}

func (storage *GroupStorage) getLastGroupReceivedID(appid int64, gid int64, uid int64, did int64) (int64, error) {
	primaryKey := &OTSPrimaryKey{
		"gid" : gid,
		"uid" : uid,
		"deviceid" : did,
	}
	
	columnsToGet := &OTSColumnsToGet{
		"msgid",
	}
	
	get_row_response, err := storage.ots2_client.GetRow("msg_group_user_last_recv_id", primaryKey, columnsToGet)
	if err != nil {
		return 0, err
	}
	
	if get_row_response.Row != nil {
		if attributeColumns := get_row_response.Row.GetAttributeColumns(); attributeColumns != nil {
			msgid := attributeColumns.Get("msgid").(int64)
			return msgid, nil
		}
	}
	
	return 0, nil
}

func (storage *GroupStorage) GetLastGroupReceivedID(appid int64, gid int64, uid int64, device_id int64) (int64, error) {
	storage.mutex.Lock()
	defer storage.mutex.Unlock()
	return storage.getLastGroupReceivedID(appid, gid, uid, device_id)
}

func (storage *GroupStorage) loadRangeMessages(gid int64, minid int64, maxid int64) []*Message {
	startPrimaryKey := &OTSPrimaryKey{
		"gid" : gid,
		"msgid" : minid,
	}
	
	endPrimaryKey := &OTSPrimaryKey{
		"gid" : gid,
		"msgid" : maxid+1,
	}
	
	columnsToGet := &OTSColumnsToGet{
		"cmd", "seq", "version", "body",
	}
	
	msgs := make([]*Message, 0, 10)
	
	response_row_list, err := storage.ots2_client.GetRange("msg_group", OTSDirection_BACKWARD, startPrimaryKey, endPrimaryKey, columnsToGet, 100)
	if err != nil {
		log.Infoln(err)
		return msgs
	}
	
	if response_row_list.GetRows() == nil {
		return msgs
	}
	
	for _, v := range response_row_list.GetRows() {
		if attributeColumns := v.GetAttributeColumns(); attributeColumns != nil {
			cmd := attributeColumns.Get("cmd").(int)
			seq := attributeColumns.Get("seq").(int)
			version := attributeColumns.Get("version").(int)
			body := attributeColumns.Get("body").(string)
			
			immsg := IMMessage{}
			json_err := json.Unmarshal([]byte(body), &immsg)
			if json_err == nil {
				msg := Message{}
				msg.cmd = cmd
				msg.seq = seq
				msg.version = version
				msg.body = &immsg
				
				msgs = append(msgs, &msg)
			}
		}
	}
	
	return msgs
}

func (storage *GroupStorage) LoadRangeMessages(gid int64, minid int64, maxid int64) []*Message {
	storage.mutex.Lock()
	defer storage.mutex.Unlock()
	
	return storage.loadRangeMessages(gid, minid, maxid)
}

func (storage *GroupStorage) LoadGroupOfflineMessage(appid int64, gid int64, uid int64, device_id int64) []*EMessage {
	last_id, err := storage.GetLastGroupMessageID(appid, gid)
	if err != nil {
		log.Info("get last group message id err:", err)
		return nil
	}

	last_received_id, _ := storage.GetLastGroupReceivedID(appid, gid, uid, device_id)
	msgs := storage.LoadRangeMessages(gid, last_received_id, last_id)

	c := make([]*EMessage, 0, 10)
	for _, msg := range msgs {
		m := msg.body.(*IMMessage)
		c = append(c, &EMessage{msgid: m.msgid, device_id: device_id, msg: msg})
	}

	if len(c) > 0 {
		//reverse
		size := len(c)
		for i := 0; i < size/2; i++ {
			t := c[i]
			c[i] = c[size-i-1]
			c[size-i-1] = t
		}
	}

	log.Infof("load group offline message appid:%d gid:%d uid:%d count:%d last id:%d last received id:%d\n", appid, gid, uid, len(c), last_id, last_received_id)
	return c
}

func (storage *GroupStorage) DequeueGroupOffline(msgid int64, appid int64, gid int64, receiver int64, device_id int64) {
	storage.mutex.Lock()
	defer storage.mutex.Unlock()
	
	storage.setLastGroupReceivedID(appid, gid, receiver, device_id, msgid)
}