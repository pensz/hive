package org.apache.hadoop.hive.hwi.model;

import java.util.Date;

public class MQuery {

  public enum Status {INITED, RUNNING, FINISHED, CANCELLED, FAILED, SYNTAXERROR};

  private Integer id;

  private String name;

  private String query;

  private String resultLocation;

  private Status status;

  private String errorMsg;

  private String description;

  private String callback;

  private String userId;

  private Date created;

  private Date updated;

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public String getQuery() {
    return query;
  }

  public void setQuery(String query) {
    this.query = query;
  }

  public String getResultLocation() {
    return resultLocation;
  }

  public void setResultLocation(String resultLocation) {
    this.resultLocation = resultLocation;
  }

  public Status getStatus() {
    return status;
  }

  public void setStatus(Status status) {
    this.status = status;
  }

  public String getErrorMsg() {
    return errorMsg;
  }

  public void setErrorMsg(String errorMsg) {
    this.errorMsg = errorMsg;
  }

  public String getDescription() {
    return description;
  }

  public void setDescription(String description) {
    this.description = description;
  }

  public String getCallback() {
    return callback;
  }

  public void setCallback(String callback) {
    this.callback = callback;
  }

  public String getUserId() {
    return userId;
  }

  public void setUserId(String userId) {
    this.userId = userId;
  }

  public Date getCreated() {
    return created;
  }

  public void setCreated(Date created) {
    this.created = created;
  }

  public Date getUpdated() {
    return updated;
  }

  public void setUpdated(Date updated) {
    this.updated = updated;
  }

  public Integer getId() {
    return id;
  }

  public void setId(Integer id) {
    this.id = id;
  }

}
