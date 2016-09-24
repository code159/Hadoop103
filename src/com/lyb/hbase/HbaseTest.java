package com.lyb.hbase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;

public class HbaseTest {
	//声明静态配置
	private static Configuration hconf=null;
	static{
		hconf=HBaseConfiguration.create();
		hconf.clear();
		hconf.addResource("conf/hbase-default.xml");
		hconf.addResource("conf/hbase-site.xml");
		hconf.set("hbase.zookeeper.quorum", "172.168.2.9");	
	}

	
	public static void main(String[] args) throws IOException {
			
		System.out.println(hconf.get("hbase.zookeeper.property.clientPort"));
			

			// 管理员
	        @SuppressWarnings("resource")
			HBaseAdmin hbaseAdmin = new HBaseAdmin(hconf);
	        	        

			  // 表名称
	        String tableName="t10";
	        	
	        
	        //建表
	        if(hbaseAdmin.tableExists(tableName)){
	        	System.out.println(tableName+" exists!need to be recreated!");
	        	hbaseAdmin.disableTable(tableName);
	        	hbaseAdmin.deleteTable(tableName);
	        }
	        
	        // 表描述器
	        HTableDescriptor tableDesc = new HTableDescriptor(tableName);

	        tableDesc.addFamily(new HColumnDescriptor("f1"));// 添加列族
	        hbaseAdmin.createTable(tableDesc);
	        System.out.println("创建表" + tableName + "成功");

	        //插入数据
	        HTablePool hp=new HTablePool();
	        Put put=new Put("r1".getBytes());
	        put.add("f1".getBytes(), "c1".getBytes(), "1".getBytes());
	        put.add("f1".getBytes(), "c2".getBytes(), "2".getBytes());
	        put.add("f1".getBytes(), "c3".getBytes(), "3".getBytes());
	        hp.getTable(tableName).put(put);
	        put=new Put("r2".getBytes());
	        put.add("f1".getBytes(), "c1".getBytes(), "4".getBytes());
	        put.add("f1".getBytes(), "c2".getBytes(), "5".getBytes());
	        put.add("f1".getBytes(), "c3".getBytes(), "6".getBytes());
	        hp.getTable(tableName).put(put);
	        System.out.println("插入表" + tableName + "成功");
	        
	        //删除数据
	        HTable ht=new HTable(hconf, tableName.getBytes());
	        String rowKey="r1";
	        Delete d=new Delete(rowKey.getBytes());
	        List l=new ArrayList();
	        l.add(d);
	        ht.delete(l);
	        System.out.println("删除表" + tableName +"中rowKey:"+rowKey+ "成功");	 
	        
	        //单条件rowKey查询
	        Get get=new Get("r2".getBytes());
	        Result r=hp.getTable(tableName).get(get);
	        System.out.println("rowKey:"+new String(r.getRow()));
	        for(KeyValue keyValue:r.raw()){
		        System.out.println("family:"+new String(keyValue.getFamily())+" column:"+new String(keyValue.getQualifier())+" value:"+new String(keyValue.getValue()));
	        }
	}
}
