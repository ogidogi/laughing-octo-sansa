package org.bidsup.engine.beans;

import java.io.Serializable;
import java.util.Date;
import java.util.UUID;

public class Tweet implements Serializable {

    private static final long serialVersionUID = 6464859688245535942L;

    private UUID key;
    private String user;
    private String text;
    private Date createDate;

    // Remember to declare no-args constructor
    public Tweet() {
        key = UUID.randomUUID();
    }

    public Tweet(String user, String text, Date createDate) {
        this.user = user;
        this.text = text;
        this.createDate = createDate;
        key = UUID.randomUUID();
    }

    public UUID getKey() {
        return key;
    }

    public void setKey(UUID key) {
        this.key = key;
    }

    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public String getText() {
        return text;
    }

    public void setText(String text) {
        this.text = text;
    }

    public Date getCreateDate() {
        return createDate;
    }

    public void setCreateDate(Date createDate) {
        this.createDate = createDate;
    }

    @Override
    public String toString() {
        return "TweetRowWithUUID{" + "key=" + key + ", text='" + text + ", createDate" + createDate + '}';
    }
}
