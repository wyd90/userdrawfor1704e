package com.bawei.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

//  /ordercaulate/2020/3/24/9/
//  2020 3 24 9
public class MergeFile {
    public static void main(String[] args) throws IOException, InterruptedException {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(URI.create("hdfs://node4:8020"), conf, "root");

        String year = args[0];
        String month = args[1];
        String day = args[2];
        String hour = args[3];

        List<Path> paths = new ArrayList<>();
        listFile(new Path("hdfs://node4:8020/ordercaulate/"+year+"/"+month+"/"+day+"/"+hour),fs,paths);
        int i = 0;
        for(Path path : paths) {
            if(!path.toString().contains("_SUCCESS")) {
                FileUtil.copy(fs,path,fs,new Path("hdfs://node4:8020/ordercaulatemid/"+year+"-"+month+"-"+day+"-"+hour+"/"+i),true,conf);
                i++;
            }
        }
        FileUtil.copyMerge(fs,new Path("hdfs://node4:8020/ordercaulatemid/"+year+"-"+month+"-"+day+"-"+hour),fs, new Path("hdfs://node4:8020/caulateres/"+year+"/"+month+"/"+day+"/"+hour),true,conf,null);
        fs.delete(new Path("hdfs://node4:8020/ordercaulatemid/"+year+"-"+month+"-"+day+"-"+hour),true);
        fs.delete(new Path("hdfs://node4:8020/ordercaulate/"+year+"/"+month+"/"+day+"/"+hour),true);
    }

    public static void listFile(Path path, FileSystem fs, List<Path> filePaths) throws IOException {
        FileStatus[] list = fs.listStatus(path);
        for(FileStatus fileStatus : list) {
            if(fileStatus.isDirectory()) {
                listFile(fileStatus.getPath(),fs,filePaths);
            } else {
                filePaths.add(fileStatus.getPath());
            }
        }
    }
}
