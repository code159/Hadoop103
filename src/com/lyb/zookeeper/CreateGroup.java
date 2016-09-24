package com.lyb.zookeeper;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;

public class CreateGroup implements Watcher{

	private static final int SESSION_TIMEOUT=5000;
	private ZooKeeper zk;
	private CountDownLatch connectedSignal=new CountDownLatch(1);
	
	public void connect(String hosts) throws IOException, InterruptedException{
		zk=new ZooKeeper(hosts,SESSION_TIMEOUT,this);
		connectedSignal.await();
	}
	
	@Override
	public void process(WatchedEvent event) {
		if(event.getState()==KeeperState.SyncConnected){
			connectedSignal.countDown();
		}
	}
	
	public void create(String groupName) throws KeeperException, InterruptedException{
		String path="/"+groupName;
		if(zk.exists(path, true)==null){
			System.out.println("节点"+path+"不存在");
			String createdPath=zk.create(path, "byAPI".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
			System.out.println("created"+createdPath);
		}else{
			System.out.println("节点"+path+"已存在，先删除此节点及所有子节点");
			delete(groupName);
			create(groupName);
		}
	}
	
	//现在只能删除单级或两级节点。。待研究
	public void delete(String groupName) throws KeeperException, InterruptedException{
		String path="/"+groupName;
		try{
			List<String> children=zk.getChildren(path, true);
			for(String child:children){
				zk.delete(path+"/"+child, -1);
			}
			zk.delete(path, -1);
		}catch(KeeperException.NoNodeException e){
			System.out.printf("Group %s does not exists\n",groupName);
			System.exit(1);
		}
	}
	
	public void list(String groupName) throws KeeperException, InterruptedException{
		String path="/"+groupName;
		try{
			List<String> children=zk.getChildren(path, true);
			if(children.isEmpty()){
				System.out.printf("No members in group %s\n",groupName);
				System.exit(1);
			}
			for(String child:children){
				System.out.println(child);
			}
			
		}catch(KeeperException.NoNodeException e){
			System.out.printf("Group %s does not exists\n",groupName);
		}
	}
	
	public void get(String groupName) throws KeeperException, InterruptedException{
		String path="/"+groupName;
		System.out.println("节点"+path+"值为"+new String(zk.getData(path, false, null)));
	}
	
	public void close() throws InterruptedException{
		zk.close();
	}

	
	public static void main(String[] args) throws IOException, InterruptedException, KeeperException {
		CreateGroup cg=new CreateGroup();
		cg.connect("172.168.2.9:2183");
		cg.create("byAPI");
		cg.create("byAPI/child1");
//		cg.create("byAPI/child1/child2");
		
		cg.list("byAPI");
		cg.get("byAPI/child1");
		
		cg.close();
		
	}

	
}
