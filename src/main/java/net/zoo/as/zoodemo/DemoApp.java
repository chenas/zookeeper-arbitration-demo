package net.zoo.as.zoodemo;

public class DemoApp {
	

    private static final String HOST = "localhost:2181";
    private static final String ZNODE_DATA_PATH = "/test_data";
    private static final String ZNODE_MASTER_PATH = "/test_master";
        
    public static void main(String[] args){
    	
    	ZkController ctr = new ZkController();
    	ctr.init(HOST, ZNODE_DATA_PATH, ZNODE_MASTER_PATH);
    	
        int count =0;
        while(true){
        	count++;
        	String data = "this is test "+String.valueOf(count);
        	ctr.updateData(ZNODE_DATA_PATH, data.getBytes());
        	try {
				Thread.sleep(1000* 10);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
        }
    }

}
