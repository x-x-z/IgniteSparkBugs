package org.xxz.ignite_spark_bugs;

import org.apache.ignite.cache.query.annotations.QuerySqlField;

import java.io.Serializable;

public class Person implements Serializable {
    private static final long serialVersionUID = -4052172169584874312L;

    @QuerySqlField
    private Long id;

    @QuerySqlField(index = true)
    private String name;

    public Person(Long id, String name) {
        this.id = id;
        this.name = name;
    }

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }
}
