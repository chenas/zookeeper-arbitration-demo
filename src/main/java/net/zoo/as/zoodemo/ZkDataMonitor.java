package net.zoo.as.zoodemo;

import java.util.Arrays;

import org.apache.zookeeper.AsyncCallback.StatCallback;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ZkDataMonitor implements Watcher, StatCallback{

	private static Logger logger = LoggerFactory.getLogger(ZkDataMonitor.class);
	
	private ZkMessageListener listener;	
	private ZooKeeper zk;
	private byte preData[];
	
	// 监听回调接口，消息通知接口
	public interface ZkMessageListener{
		
		public void onDataChanged(String znode, byte[] data);
		public void onNodeDeleted(String znode);
		
		public void close(int statucode);
	}
	
	 public ZkDataMonitor(ZooKeeper zk, ZkMessageListener listener, String ...znodes ){
		 this.listener = listener;
		 this.zk = zk;
		 for (String node : znodes){
			 this.zk.exists(node, true, this, null);
		 }
	 }

	// implements from Watcher
	public void process(WatchedEvent event) {
		logger.info( "收到Watcher通知: {}", event.getPath());
		logger.info( "连接状态:\t" + event.getState().toString() );
		logger.info( "事件类型:\t" + event.getType().toString() );
		
		String path = event.getPath();
        if (event.getType() == Event.EventType.None) {
            // We are are being told that the state of the
            // connection has changed
            switch (event.getState()) {
            case SyncConnected:
                // In this particular example we don't need to do anything
                // here - watches are automatically re-registered with 
                // server and any watches triggered while the client was 
                // disconnected will be delivered (in order of course)
                break;
            case Expired:
                // It's all over
                listener.close(KeeperException.Code.SessionExpired);
                break;
			default:
				break;
            }
        }else if(event.getType() == Event.EventType.NodeDeleted){
        	listener.onNodeDeleted(path);
        }else {
            if (path != null) {
                // Something has changed on the node, let's find out
                zk.exists(path, true, this, null);
            }
        }
	}

	//implements from StatCallback
	public void processResult(int rc, String path, Object ctx, Stat stat) {
		switch(rc){
		case KeeperException.Code.Ok:
			byte data[] = null;
	            try {
	            	data = zk.getData(path, false, null);
	            } catch (KeeperException |InterruptedException e) {
	                e.printStackTrace();
	            } 
	        if ((data == null && data != preData)
	                || (data != null && !Arrays.equals(preData, data))) {
	            listener.onDataChanged(path, data);
	            preData = data;
	        }
			break;
		case KeeperException.Code.BadVersion:
			listener.close(KeeperException.Code.BadVersion);
			break;
		default:
			zk.exists(path, true, this, null);
			break;
		}
	}

}
