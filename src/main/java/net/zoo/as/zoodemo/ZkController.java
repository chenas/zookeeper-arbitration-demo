package net.zoo.as.zoodemo;

import java.io.IOException;


import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.ZooDefs.Ids;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ZkController implements Watcher, ZkDataMonitor.ZkMessageListener{
	
	private static Logger logger = LoggerFactory.getLogger(ZkController.class);
	
	public enum Status{
		Active,
		StandBy
	}
	
	public Status status = Status.StandBy;
	public ZkDataMonitor monitor;
	public String zkHost;
	public String dataNode;  // master往里写数据，slave则监听读取数据
	public String masterNode;  //创建成功则为master，其他未slave
	public ZooKeeper zooKeeper;
	
	public ZkDataMonitor dataMonitor;
	
	public boolean init(String zkHost, String dataNode, String masterNode){
		this.zkHost = zkHost;
		this.dataNode = dataNode;
		this.masterNode = masterNode;
		try {
			zooKeeper = new ZooKeeper(zkHost, 10000, this);
			dataMonitor = new ZkDataMonitor(zooKeeper, this, dataNode, masterNode);
		} catch (IOException e) {
			e.printStackTrace();
		}
		try {
			String createdPath = zooKeeper.create(this.masterNode, "master".getBytes(), Ids.READ_ACL_UNSAFE, CreateMode.EPHEMERAL);
			if(createdPath != null){
				this.status = Status.Active;  // become a master
			}
		} catch (KeeperException | InterruptedException e) {
			this.status = Status.StandBy;
			e.printStackTrace();
		}
		logger.info("Controller status: {}", this.status);
		try {
			zooKeeper.create(this.dataNode, "".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);				
		} catch (KeeperException | InterruptedException e) {
			e.printStackTrace();
		}
		return true;
	}
	
	boolean updateData(String znode, byte[] data){
		if(this.status == Status.StandBy)
			return false;
		try {
			zooKeeper.setData(znode, data, -1);
		} catch (KeeperException | InterruptedException e) {
			e.printStackTrace();
			return false;
		}
		return true;
	}

	// implements from Watcher
	public void process(WatchedEvent event) {
		dataMonitor.process(event);		
	}

	public void close(int statucode) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void onNodeDeleted(String znode) {
		logger.debug("deleted path: {}", znode);
		if(znode.equals(this.masterNode)){
			try {
				String createdPath = zooKeeper.create(this.masterNode, "master".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
				if(createdPath != null){
					this.status = Status.Active;  // become a master
				}
				logger.info("Controller status: {}", this.status);
			} catch (KeeperException | InterruptedException e) {
				this.status = Status.StandBy;
				e.printStackTrace();
			}
		}else if(znode.equals(this.dataNode)){
			try {
				zooKeeper.create(this.dataNode, "".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);				
			} catch (KeeperException | InterruptedException e) {
				e.printStackTrace();
			}
		}
		
	}

	@Override
	public void onDataChanged(String znode, byte[] data) {
		logger.info("receive data from path: " + znode + " content: " + new String(data));
	}

}
