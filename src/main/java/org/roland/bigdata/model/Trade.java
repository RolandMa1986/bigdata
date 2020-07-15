package org.roland.bigdata.model;

import lombok.Data;

import java.sql.Timestamp;

@Data
public class Trade {

    private long id;
    private long userId;
    private Timestamp payTime;
    private double amount;
}
