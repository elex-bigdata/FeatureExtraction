package com.elex.ssp.common;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.IOUtils;

import com.google.common.io.Closeables;

public class HdfsUtil {
	
	public static int countRecords(Path path,Configuration conf) throws IOException{
		BufferedReader br;
		int count = 0;
        FileSystem fs = FileSystem.get(conf);
        FileStatus[] files = fs.listStatus(path, new PathFilter() {
		    @Override
		    public boolean accept(Path path) {
		      String name = path.getName();
		      return name.startsWith("part-") && !name.endsWith(".crc");
		    }
		  });
        
        for(FileStatus file:files){
        	
        	if(!file.isDirectory()){
        		br=new BufferedReader(new InputStreamReader(fs.open(file.getPath())));
        		while(br.readLine()!=null){
        			count++;
        		}   
        		br.close();
        	}        	
        }
		
		return count;
	}
	
	public static void writeInt(int value, Path path, Configuration conf) throws IOException {
	    FileSystem fs = FileSystem.get(path.toUri(), conf);
	    FSDataOutputStream out = fs.create(path);
	    try {
	      out.writeInt(value);
	    } finally {
	      Closeables.closeQuietly(out);
	    }
	  }
	
	public static int readInt(Path path, Configuration conf) throws IOException {
	    FileSystem fs = FileSystem.get(path.toUri(), conf);
	    FSDataInputStream in = fs.open(path);
	    try {
	      return in.readInt();
	    } finally {
	      Closeables.closeQuietly(in);
	    }
	  }

	public static void upFile(FileSystem fs,Configuration conf,String localFile,String hdfsPath) throws IOException{
		InputStream in=new BufferedInputStream(new FileInputStream(localFile));
		OutputStream out=fs.create(new Path(hdfsPath));
		IOUtils.copyBytes(in, out, conf);
		out.close();
		in.close();

	}

	public static void downFile(FileSystem fs,Configuration conf,String hdfsPath, String localPath) throws IOException{
		InputStream in=fs.open(new Path(hdfsPath));
		OutputStream out=new FileOutputStream(localPath);
		IOUtils.copyBytes(in, out, conf);
		out.close();
		in.close();
		
	}
	
	public static void makeDir(FileSystem fs,String hdfsPath) throws IOException{
		Path path = new Path(hdfsPath);
		if (fs.exists(path)) {
	        fs.delete(path, true);
	      }
		fs.mkdirs(path);
	}

	public static void delFile(FileSystem fs,String hdfsPath) throws IOException{
		fs.delete(new Path(hdfsPath), true);
	}
	
	public static void backupFile(FileSystem fs,Configuration conf,String src,String dst) throws IOException{
		Path srcP = new Path(src);
		Path dstP = new Path(dst);
		org.apache.hadoop.fs.FileUtil.copy(fs, srcP, fs, dstP, true, conf);
		
	}
	
}
