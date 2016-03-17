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

import "strconv"
import "log"
import "github.com/richmonkey/cfg"

//storage参数,对应im.cfg
type StorageConfig struct {
	listen             string
	mysqldb_datasource string
	mysqldb_appdatasource string
	redis_address      string
	redis_password		string
	
	ots_endpoint string
	ots_accessid string
	ots_accesskey string
	ots_instancename string
}

func get_int(app_cfg map[string]string, key string) int {
	concurrency, present := app_cfg[key]
	if !present {
		log.Fatalf("key:%s non exist", key)
	}
	n, err := strconv.Atoi(concurrency)
	if err != nil {
		log.Fatalf("key:%s is't integer", key)
	}
	return n
}

//读取配置方法,从map中找对应key的value
func get_string(app_cfg map[string]string, key string) string {
	concurrency, present := app_cfg[key]
	if !present {
		log.Fatalf("key:%s non exist", key)
	}
	return concurrency
}

func get_opt_string(app_cfg map[string]string, key string) string {
	concurrency, present := app_cfg[key]
	if !present {
		return ""
	}
	return concurrency
}

func read_storage_cfg(cfg_path string) *StorageConfig {
	config := new(StorageConfig)
	app_cfg := make(map[string]string)
	//引用包github.com/richmonkey/cfg 读取配置
	err := cfg.Load(cfg_path, app_cfg)
	if err != nil {
		log.Fatal(err)
	}

	config.listen = get_string(app_cfg, "listen")
	config.redis_address = get_string(app_cfg, "redis_address")
	config.redis_password = get_string(app_cfg, "redis_password")
	config.mysqldb_datasource = get_string(app_cfg, "mysqldb_source")
	config.mysqldb_appdatasource = get_string(app_cfg, "mysqldb_appsource")
	
	config.ots_endpoint = get_string(app_cfg, "ots_endpoint")
	config.ots_accessid = get_string(app_cfg, "ots_accessid")
	config.ots_accesskey = get_string(app_cfg, "ots_accesskey")
	config.ots_instancename = get_string(app_cfg, "ots_instancename")

	return config
}
