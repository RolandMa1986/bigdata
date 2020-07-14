package org.roland.bigdata.model;

import java.io.Serializable;
import lombok.Data;

@Data
public class User {
    private long id;
    private String name;
    private Integer age;
}
