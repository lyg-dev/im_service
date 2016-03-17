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
	"encoding/json"
	"fmt"
	"sync"
)

import log "github.com/golang/glog"
import ots2 "github.com/GiterLab/goots"
//import "github.com/GiterLab/goots/log"
import . "github.com/GiterLab/goots/otstype"

type PeerStorage struct {
	ots2_client *ots2.OTSClient
	mutex sync.Mutex
}

func NewPeerStorage(ots2_client *ots2.OTSClient) *PeerStorage {
	storage := &PeerStorage{}
	storage.ots2_client = ots2_client
	return storage
}

func (storage *PeerStorage) saveMessage(msg *Message) int64 {
	m := msg.body.(*IMMessage)
	
	msgid, err := iw.NextId()
	if err != nil {
		log.Fatalln(err)
	}

	m.msgid = msgid
	
	primaryKey := &OTSPrimaryKey{
		"uid" : m.receiver,
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
	_, err = storage.ots2_client.PutRow("msg_user", condition, primaryKey, attributeColumns)
	if err != nil {
		log.Infoln(err)
		return 0
	}
	
	return msgid
}

func (storage *PeerStorage) SavePeerMessage(appid int64, uid int64, device_id int64, msg *Message) int64 {
	storage.mutex.Lock()
	defer storage.mutex.Unlock()
	//写入message
	msgid := storage.saveMessage(msg)

	//设置用户最近一条消息id
	storage.setLastMessageID(appid, uid, msgid)
	return msgid
}

func (storage *PeerStorage) getLastMessageID(appid int64, receiver int64) (int64, error) {
	primaryKey := &OTSPrimaryKey{
		"uid" : receiver,
	}
	
	columnsToGet := &OTSColumnsToGet{
		"msgid",
	}
	
	get_row_response, err := storage.ots2_client.GetRow("msg_user_last_id", primaryKey, columnsToGet)
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

//获取最近消息ID
func (storage *PeerStorage) GetLastMessageID(appid int64, receiver int64) (int64, error) {
	storage.mutex.Lock()
	defer storage.mutex.Unlock()
	
	return storage.getLastMessageID(appid, receiver)
}

func (storage *PeerStorage) setLastMessageID(appid int64, receiver int64, msgid int64) {
	primaryKey := &OTSPrimaryKey{
		"uid" : receiver,
	}
	
	attributeColumns := &OTSAttribute{
		"msgid" : msgid,
	}
	
	condition := OTSCondition_IGNORE
	_, err := storage.ots2_client.PutRow("msg_user_last_id", condition, primaryKey, attributeColumns)
	if err != nil {
		log.Infoln(err)
	}
}

//设置最新消息ID
func (storage *PeerStorage) SetLastMessageID(appid int64, receiver int64, msgid int64) {	
	storage.mutex.Lock()
	defer storage.mutex.Unlock()
	
	storage.setLastMessageID(appid, receiver, msgid)
}

func (storage *PeerStorage) setLastReceivedID(appid int64, uid int64, did int64, msgid int64) {
	primaryKey := &OTSPrimaryKey{
		"uid" : uid,
		"deviceid" : did,
	}
	
	attributeColumns := &OTSAttribute{
		"msgid" : msgid,
	}
	
	condition := OTSCondition_IGNORE
	_, err := storage.ots2_client.PutRow("msg_user_last_recv_id", condition, primaryKey, attributeColumns)
	if err != nil {
		log.Infoln(err)
	}
}

//设置最后一条已接收的msgid
func (storage *PeerStorage) SetLastReceivedID(appid int64, uid int64, did int64, msgid int64) {
	storage.mutex.Lock()
	defer storage.mutex.Unlock()
	
	storage.setLastReceivedID(appid, uid, did, msgid)	
}

func (storage *PeerStorage) getLastReceivedID(appid int64, uid int64, did int64) (int64, error) {
	primaryKey := &OTSPrimaryKey{
		"uid" : uid,
		"deviceid" : did,
	}
	
	columnsToGet := &OTSColumnsToGet{
		"msgid",
	}
	
	get_row_response, err := storage.ots2_client.GetRow("msg_user_last_recv_id", primaryKey, columnsToGet)
	if err != nil {
		return 0, err
	}
	
	if get_row_response.Row != nil {
		if attributeColumns := get_row_response.Row.GetAttributeColumns(); attributeColumns != nil {
			msgid := attributeColumns.Get("msgid").(int64)
			if err != nil {
				return 0, err
			}
			return msgid, nil
		}
	}
	
	return 0, nil
}

//获取最近接收msgid
func (storage *PeerStorage) GetLastReceivedID(appid int64, uid int64, did int64) (int64, error) {
	storage.mutex.Lock()
	defer storage.mutex.Unlock()
	
	return storage.getLastReceivedID(appid, uid, did)
}

func (storage *PeerStorage) loadRangeMessages(uid int64, minid int64, maxid int64) []*Message {
	startPrimaryKey := &OTSPrimaryKey{
		"uid" : uid,
		"msgid" : minid,
	}
	
	endPrimaryKey := &OTSPrimaryKey{
		"uid" : uid,
		"msgid" : maxid+1,
	}
	
	columnsToGet := &OTSColumnsToGet{
		"cmd", "seq", "version", "body",
	}
	
	msgs := make([]*Message, 0, 10)
	
	response_row_list, ots_err := storage.ots2_client.GetRange("msg_user", OTSDirection_BACKWARD, startPrimaryKey, endPrimaryKey, columnsToGet, 10000)
	if ots_err != nil {
		fmt.Println(ots_err)
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
			err := json.Unmarshal([]byte(body), &immsg)
			if err == nil {
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

func (storage *PeerStorage) LoadRangeMessages(uid int64, minid int64, maxid int64) []*Message {
	storage.mutex.Lock()
	defer storage.mutex.Unlock()
	
	return storage.loadRangeMessages(uid, minid, maxid)
}

//读取离线消息
func (storage *PeerStorage) LoadOfflineMessage(appid int64, uid int64, did int64) []*EMessage {
	last_id, err := storage.GetLastMessageID(appid, uid)
	if err != nil {
		return nil
	}

	last_received_id, _ := storage.GetLastReceivedID(appid, uid, did)

	log.Infof("last id:%d last received id:%d", last_id, last_received_id)
	msgs := storage.LoadRangeMessages(uid, last_received_id, last_id)
	
	c := make([]*EMessage, 0, 10)
	for _, msg := range msgs {
		m := msg.body.(*IMMessage)
		c = append(c, &EMessage{msgid: m.msgid, device_id: did, msg: msg})
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

	log.Infof("load offline message appid:%d uid:%d count:%d\n", appid, uid, len(c))
	return c
}

func (storage *PeerStorage) DequeueOffline(msgid int64, appid int64, receiver int64, device_id int64) {
	storage.mutex.Lock()
	defer storage.mutex.Unlock()
	
	storage.setLastReceivedID(appid, receiver, device_id, msgid)
}
