package org.roland.bigdata;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@EnableScheduling
@SpringBootApplication
public class BigdataLauncher {
    public static void main(String[] args) {
        SpringApplication.run(BigdataLauncher.class, args);
    }
}
