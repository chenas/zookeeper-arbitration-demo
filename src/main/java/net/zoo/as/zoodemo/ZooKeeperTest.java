package net.zoo.as.zoodemo;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
  
public class ZooKeeperTest {
  
    private static final int TIME_OUT = 3000;
    private static final String HOST = "localhost:2181";
    private static String path = "/test_data";
    public static void main(String[] args) throws Exception{
 
 
        ZooKeeper zookeeper = new ZooKeeper(HOST, TIME_OUT, null);
        System.out.println("=========创建节点===========");
        if(zookeeper.exists(path, false) == null)
        {
            zookeeper.create(path, "znode1".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        }
        System.out.println("=============查看节点是否安装成功===============");
        System.out.println(new String(zookeeper.getData(path, false, null)));
         
        System.out.println("=========修改节点的数据==========");
        String data = "zNode2";
        zookeeper.setData(path, data.getBytes(), -1);
         
        System.out.println("========查看修改的节点是否成功=========");
        System.out.println(new String(zookeeper.getData(path, false, null)));
         
        System.out.println("=======删除节点==========");
        zookeeper.delete(path, -1);
         
        System.out.println("==========查看节点是否被删除============");
        System.out.println("节点状态：" + zookeeper.exists(path, false));
         
        zookeeper.close();
    } 
}