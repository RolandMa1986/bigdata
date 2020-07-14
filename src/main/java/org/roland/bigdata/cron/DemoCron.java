package org.roland.bigdata.cron;

import org.apache.commons.lang.ObjectUtils;
import org.apache.spark.launcher.SparkLauncher;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;

@Component
public class DemoCron {

    @Scheduled(cron = "0 0/1 * * * ?")
    // @SchedulerLock(name = "DemoCron")
    public void run() {
        SparkLauncher sparkLauncher = new SparkLauncher();

        sparkLauncher.setSparkHome("/opt/spark-3.0.0-bin-hadoop3.2")
                .setAppResource("/home/user/github.com/roland/bigdata/target/bigdata-1.0-SNAPSHOT.jar")
                .setMainClass("org.roland.bigdata.spark.LoadData")
                .setMaster("local[*]")
                .setDeployMode("client")
                .setConf("spark.executor.cores", "2")
                .setAppName("APPName")
                .addAppArgs("");

        try {
            Process process = sparkLauncher.launch();
            InputStream stream = process.getInputStream();
            printStream(stream);
            InputStream errorstream = process.getErrorStream();
            printStream(errorstream);
            stream.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    private void printStream(InputStream inputStream) {
        InputStreamReader reader = null;
        try {
            reader = new InputStreamReader(inputStream, "UTF-8");
            StringBuffer sb = new StringBuffer();
            while (null != reader && reader.ready()) {
                sb.append((char) reader.read());
                // 转成char加到StringBuffer对象中
            }
            System.out.println(sb.toString());
            reader.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}

