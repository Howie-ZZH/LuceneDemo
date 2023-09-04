package com.howie.lucenedemo;

import com.howie.lucenedemo.Config.DemoService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class LuceneDemoApplication {

    @Autowired
    DemoService demoService;



}
