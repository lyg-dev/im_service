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
import "strings"
import "github.com/richmonkey/cfg"

type Config struct {
	port                  int
	mysqldb_datasource    string
	mysqldb_appdatasource string

	redis_address       string
	http_listen_address string
	socket_io_address   string

	storage_addrs []string
	route_addrs   []string
}

//storage参数,对应im.cfg
type StorageConfig struct {
	listen             string
	storage_root       string
	mysqldb_datasource string
	redis_address      string
	sync_listen        string
	master_address     string
}

type RouteConfig struct {
	listen        string
	redis_address string
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

func read_cfg(cfg_path string) *Config {
	config := new(Config)
	app_cfg := make(map[string]string)
	err := cfg.Load(cfg_path, app_cfg)
	if err != nil {
		log.Fatal(err)
	}

	config.port = get_int(app_cfg, "port")
	config.http_listen_address = get_string(app_cfg, "http_listen_address")
	config.redis_address = get_string(app_cfg, "redis_address")
	config.mysqldb_datasource = get_string(app_cfg, "mysqldb_source")
	config.mysqldb_appdatasource = get_string(app_cfg, "mysqldb_appsource")
	config.socket_io_address = get_string(app_cfg, "socket_io_address")

	str := get_string(app_cfg, "storage_pool")
	array := strings.Split(str, " ")
	config.storage_addrs = array
	if len(config.storage_addrs) == 0 {
		log.Fatal("storage pool config")
	}

	str = get_string(app_cfg, "route_pool")
	array = strings.Split(str, " ")
	config.route_addrs = array
	if len(config.route_addrs) == 0 {
		log.Fatal("route pool config")
	}

	return config
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
	config.storage_root = get_string(app_cfg, "storage_root")
	config.redis_address = get_string(app_cfg, "redis_address")
	config.mysqldb_datasource = get_string(app_cfg, "mysqldb_source")
	config.sync_listen = get_string(app_cfg, "sync_listen")
	config.master_address = get_opt_string(app_cfg, "master_address")
	return config
}

func read_route_cfg(cfg_path string) *RouteConfig {
	config := new(RouteConfig)
	app_cfg := make(map[string]string)
	err := cfg.Load(cfg_path, app_cfg)
	if err != nil {
		log.Fatal(err)
	}

	config.listen = get_string(app_cfg, "listen")
	config.redis_address = get_string(app_cfg, "redis_address")
	return config
}
