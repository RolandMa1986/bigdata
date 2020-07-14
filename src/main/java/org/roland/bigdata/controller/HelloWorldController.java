package org.roland.bigdata.controller;

import lombok.AllArgsConstructor;
import org.springframework.web.bind.annotation.*;

@RestController
@AllArgsConstructor
@RequestMapping("/")
public class HelloWorldController {

    @GetMapping
    public String sayHello(){
        return "hello world";
    }
}
