package com.hadoop;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.LineReader;

public class Utils {
    // convert text from data file to double value;data transform
    public static ArrayList<Double> textToArrayForDataFile(Text text) {
        ArrayList<Double> list = new ArrayList<>();
        int dataIndex = text.toString().indexOf(":");
        String data = text.toString().substring(dataIndex+1);
        String[] dataText = data.split(" ");
        for (String str : dataText) {
            list.add(Double.parseDouble(str));
        }
        return list;
    }
    // convert text from center point file to double value;data transform
    public static ArrayList<Double> textToArrayForCenterFile(Text text) {
        ArrayList<Double> list = new ArrayList<>();
        int dataIndex = text.toString().indexOf(":");
        String data = text.toString().substring(dataIndex+1);
        String[] dataText = data.split(",");
        for (String str : dataText) {
            list.add(Double.parseDouble(str));
        }
        return list;
    }

    // read center from file
    public static ArrayList<ArrayList<Double>> getCenters(String centerFile, boolean isDirectory) throws IOException {
        ArrayList<ArrayList<Double>> res = new ArrayList<ArrayList<Double>>();
        Path path = new Path(centerFile);
        Configuration conf = new Configuration();

        FileSystem fileSystem = path.getFileSystem(conf);

        if (isDirectory) {
            FileStatus[] fileList = fileSystem.listStatus(path);
            for (int i = 0; i < fileList.length; ++i) {
                res.addAll(getCenters(fileList[i].getPath().toString(), false));
            }
            return res;
        }
        FSDataInputStream fsinput = fileSystem.open(path);
        LineReader lineReader = new LineReader(fsinput, conf);
        Text line = new Text();
        while (lineReader.readLine(line) > 0) {
            ArrayList<Double> lineData = textToArrayForCenterFile(line);
            res.add(lineData);
        }
        lineReader.close();
        return res;
    }

    // delete file
    public static void deletePath(String pathStr, boolean isDeleteDir) throws IOException {
        if (isDeleteDir)
            pathStr = pathStr.substring(0, pathStr.lastIndexOf('/'));
        Path path = new Path(pathStr);
        Configuration configuration = new Configuration();
        // 获取 HDFS 文件系统
        FileSystem fileSystem = path.getFileSystem(configuration);
        fileSystem.delete(path, true);
    }

    // compare centers, whether centers changed
    public static boolean compareCenters(String centerPathStr, String newPathStr) throws IOException {
        List<ArrayList<Double>> oldCenters = Utils.getCenters(centerPathStr, false);
        List<ArrayList<Double>> newCenters = Utils.getCenters(newPathStr, true);

        int size = oldCenters.size();
        int dimension = oldCenters.get(0).size();
        double distance = 0;
        for (int i = 0; i < size; ++i) {
            for (int j = 0; j < dimension; ++j) {
                double t1 = oldCenters.get(i).get(j);
                double t2 = newCenters.get(i).get(j);
                distance += Math.pow(t1 - t2, 2);
            }
        }
        if (distance == 0.0) {
            Utils.deletePath(centerPathStr, false);
            return true;
        } else {
            Configuration conf = new Configuration();
            Path outPath = new Path(centerPathStr);
            FileSystem fileSystem = outPath.getFileSystem(conf);

            FSDataOutputStream overWrite = fileSystem.create(outPath, true);
            overWrite.writeChars("");
            overWrite.close();

            Path inPath = new Path(newPathStr);
            FileStatus[] fileLists = fileSystem.listStatus(inPath);
            for(int i=0;i<fileLists.length;++i){
                FSDataOutputStream out = fileSystem.create(outPath);
                FSDataInputStream in = fileSystem.open(fileLists[i].getPath());
                IOUtils.copyBytes(in, out, 4096, true);
            }
            Utils.deletePath(newPathStr, true);
            return false;
        }
    }
}