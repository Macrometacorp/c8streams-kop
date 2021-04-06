/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.streamnative.pulsar.handlers.kop.utils;

import java.nio.charset.StandardCharsets;
import lombok.extern.slf4j.Slf4j;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;


/**
 * Utils for ZooKeeper.
 */
@Slf4j
public class ZooKeeperUtils {
	private final static String ROOT = "/authenticated_namespaces";
	
    public static void createPath(ZooKeeper zooKeeper, String subPath, byte[] data) {
        try {
            if (zooKeeper.exists(ROOT, false) == null) {
                zooKeeper.create(ROOT,  new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            }
            
            String addSubPath = ROOT + subPath;
            if (zooKeeper.exists(addSubPath, false) == null) {
                zooKeeper.create(addSubPath,
                        data, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
            } else {
                zooKeeper.setData(addSubPath, data, -1);
            }
			if (log.isDebugEnabled()) {
				log.debug("create zk path, addSubPath:{} data:{}.", addSubPath,
						new String(data, StandardCharsets.UTF_8));
			}
        } catch (Exception e) {
            log.error("create zookeeper path error", e);
        }
    }

    public static String getData(ZooKeeper zooKeeper, String subPath) {
        String data = null;
        try {
            
            String addSubPath = ROOT + subPath;
            Stat zkStat = zooKeeper.exists(addSubPath, true);
            if (zkStat != null) {
                data = new String(zooKeeper.getData(addSubPath, false, zkStat), StandardCharsets.UTF_8);
            }

			if (log.isDebugEnabled()) {
	            log.debug("get data: {} for subPath: {}", data, subPath);
	        }
            
        } catch (Exception e) {
            log.error("get zookeeper path data error", e);
        }
        return data;
    }
    
    public static void deleteNode(ZooKeeper zooKeeper, String subPath) {
        try {
			if (log.isDebugEnabled()) {
	            log.debug("Delete node: {}", subPath);
	        }
            
            String addSubPath = ROOT + subPath;
            Stat zkStat = zooKeeper.exists(addSubPath, true);
            if (zkStat != null) {
                zooKeeper.delete(addSubPath, zkStat.getVersion());
            }
        } catch (Exception e) {
            log.error("get zookeeper path data error", e);
        }
    }
}