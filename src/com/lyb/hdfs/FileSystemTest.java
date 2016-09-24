package com.lyb.hdfs;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.apache.hadoop.io.IOUtils;

public class FileSystemTest {

	static private String uri=null;
	static private InputStream in=null;
	static private OutputStream out=null;
	static private Configuration conf=null;
	static private FileStatus[] stats=null;
	static private FileStatus stat=null;

	
	public static void main(String[] args) throws IOException {
		uri="hdfs://172.168.2.9:9000/";	
		conf=new Configuration();
		FileSystem fs=FileSystem.get(URI.create(uri), conf);
		
		FileSystem fs2=new RawLocalFileSystem();
		fs2.initialize(URI.create(uri), conf);
		
		/*boolean exists=fs.exists(new Path("c:/testdir"));
		if (exists)
			fs.delete(new Path("c:/testdir"),true);
		out=fs.create(new Path("c:/testdir/testfile"));
		IOUtils.copyBytes(new ByteArrayInputStream("Hello hdfs!".getBytes()), out, 4096, true);*/
		
		/*in=fs.open(new Path("c:/testdir/eclipse.ini"));
		IOUtils.copyBytes(in, System.out, 4096, true);*/
		
		out=fs.create(new Path("/test/ha"));
		IOUtils.copyBytes(new ByteArrayInputStream("Hello hdfs!\n".getBytes()), out, 4096, true);
		
		stats=fs.listStatus(new Path("/user/hadoop"));
		for(FileStatus s:stats){
			System.out.println(s.getPath()+"\t"+s.getBlockSize()+"\t"+s.getReplication()+"\t"+(s.isDir()?"d":"f"));
		}
		
		stat=fs.getFileStatus(new Path("/user/hadoop"));
		System.out.println("group:"+stat.getGroup()+" "+stat.toString());
		

	}

}
