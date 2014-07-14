package com.basho.proserv.documentstore;

import com.basho.riak.client.convert.RiakKey;

public class ActivityLog {
    @RiakKey
    private String activityLogKey; // @RiakKey annotation required for marshalling
    private String documentId;
    private ActivityLogStatus status;

    public enum ActivityLogStatus {
        STARTED,
        COMPLETED
    }

    public ActivityLog() {
    }

    public String getActivityLogKey() {
        return activityLogKey;
    }

    public void setActivityLogKey(String activityLogKey) {
        this.activityLogKey = activityLogKey;
    }

    public String getDocumentId() {
        return documentId;
    }

    public void setDocumentId(String documentId) {
        this.documentId = documentId;
    }

    public ActivityLogStatus getStatus() {
        return status;
    }

    public void setStatus(ActivityLogStatus status) {
        this.status = status;
    }
}
