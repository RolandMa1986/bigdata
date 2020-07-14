package org.roland.bigdata.model;

import lombok.Data;
import lombok.EqualsAndHashCode;

import java.sql.Timestamp;

@Data
public class Event {

    private long id;
    private String name;
    private long userId;
    private Timestamp eventTime;

}
