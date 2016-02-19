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

import "os"
import "fmt"
import "bytes"
import "sync"
import "encoding/binary"
import "io"
import log "github.com/golang/glog"
import "github.com/syndtr/goleveldb/leveldb"
import "github.com/syndtr/goleveldb/leveldb/opt"

const HEADER_SIZE = 32
const MAGIC = 0x494d494d
const VERSION = 1 << 16 //1.0

type StorageFile struct {
	root  string
	db    *leveldb.DB
	mutex sync.Mutex
	file  *os.File
}

func NewStorageFile(root string) *StorageFile {
	storage := new(StorageFile)
	//storage的root
	storage.root = root
	//聊天记录messages的path
	path := fmt.Sprintf("%s/%s", storage.root, "messages")
	log.Info("message file path:", path)
	//读写模式/追加模式/新建模式
	file, err := os.OpenFile(path, os.O_RDWR|os.O_APPEND|os.O_CREATE, 0644)
	if err != nil {
		log.Fatal("open file:", err)
	}
	//文件大小
	file_size, err := file.Seek(0, os.SEEK_END)
	if err != nil {
		log.Fatal("seek file")
	}
	//文件头校验
	if file_size < HEADER_SIZE && file_size > 0 {
		log.Info("file header is't complete")
		err = file.Truncate(0)
		if err != nil {
			log.Fatal("truncate file")
		}
		file_size = 0
	}
	//如果是空文件,则写入文件头,magic和version
	if file_size == 0 {
		storage.WriteHeader(file)
	}
	//将文件指针赋予storage的file对象
	storage.file = file
	//离线path,离线文件存储在leveldb中
	path = fmt.Sprintf("%s/%s", storage.root, "offline")
	option := &opt.Options{}
	db, err := leveldb.OpenFile(path, option)
	if err != nil {
		log.Fatal("open leveldb:", err)
	}
	//将离线文件(leveldb)赋值给storage的db对象
	storage.db = db
	//初始化storage完成
	return storage
}

//从leveldb中读取key&value列表
func (storage *StorageFile) ListKeyValue() {
	iter := storage.db.NewIterator(nil, nil)
	for iter.Next() {
		log.Info("key:", string(iter.Key()), " value:", string(iter.Value()))
	}
}

//读取message文件
func (storage *StorageFile) ReadMessage(file *os.File) *Message {
	//校验消息起始位置的magic
	var magic int32
	err := binary.Read(file, binary.BigEndian, &magic)
	if err != nil {
		log.Info("read file err:", err)
		return nil
	}

	if magic != MAGIC {
		log.Warning("magic err:", magic)
		return nil
	}
	//从文件读取message
	msg := ReceiveMessage(file)
	if msg == nil {
		return msg
	}
	//以下代码于上面重复,应该是Bug
	err = binary.Read(file, binary.BigEndian, &magic)
	if err != nil {
		log.Info("read file err:", err)
		return nil
	}

	if magic != MAGIC {
		log.Warning("magic err:", magic)
		return nil
	}
	return msg
}

//根据msg_id读取message
func (storage *StorageFile) LoadMessage(msg_id int64) *Message {
	//锁
	storage.mutex.Lock()
	defer storage.mutex.Unlock()
	//msg_id实为文件定位,将file指针到需要的message处
	_, err := storage.file.Seek(msg_id, os.SEEK_SET)
	if err != nil {
		log.Warning("seek file")
		return nil
	}
	return storage.ReadMessage(storage.file)
}

//读取文件头
func (storage *StorageFile) ReadHeader(file *os.File) (magic int, version int) {
	header := make([]byte, HEADER_SIZE)
	n, err := file.Read(header)
	if err != nil || n != HEADER_SIZE {
		return
	}
	buffer := bytes.NewBuffer(header)
	var m, v int32
	binary.Read(buffer, binary.BigEndian, &m)
	binary.Read(buffer, binary.BigEndian, &v)
	magic = int(m)
	version = int(v)
	return
}

//写入文件头
func (storage *StorageFile) WriteHeader(file *os.File) {
	//const MAGIC = 0x494d494d
	var m int32 = MAGIC
	err := binary.Write(file, binary.BigEndian, m)
	if err != nil {
		log.Fatalln(err)
	}
	//const VERSION = 1 << 16 //1.0
	var v int32 = VERSION
	err = binary.Write(file, binary.BigEndian, v)
	if err != nil {
		log.Fatalln(err)
	}
	pad := make([]byte, HEADER_SIZE-8)
	n, err := file.Write(pad)
	if err != nil || n != (HEADER_SIZE-8) {
		log.Fatalln(err)
	}
}

//写入message
func (storage *StorageFile) WriteMessage(file io.Writer, msg *Message) {
	buffer := new(bytes.Buffer)
	binary.Write(buffer, binary.BigEndian, int32(MAGIC))
	//写入msg
	WriteMessage(buffer, msg)
	binary.Write(buffer, binary.BigEndian, int32(MAGIC))
	buf := buffer.Bytes()
	n, err := file.Write(buf)
	if err != nil {
		log.Fatal("file write err:", err)
	}
	if n != len(buf) {
		log.Fatal("file write size:", len(buf), " nwrite:", n)
	}
}

//save without lock
func (storage *StorageFile) saveMessage(msg *Message) int64 {
	msgid, err := storage.file.Seek(0, os.SEEK_END)
	if err != nil {
		log.Fatalln(err)
	}
	storage.WriteMessage(storage.file, msg)
	master.ewt <- &EMessage{msgid: msgid, msg: msg}
	log.Info("save message:", Command(msg.cmd), " ", msgid)
	return msgid

}

func (storage *StorageFile) SaveMessage(msg *Message) int64 {
	storage.mutex.Lock()
	defer storage.mutex.Unlock()
	return storage.saveMessage(msg)
}
