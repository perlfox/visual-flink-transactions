package com.example.flink.control.model;

/** Configuration submitted from the GUI's Configuration tab when starting the job. */
public class JobConfig {

    public int maxAccounts = 4000;
    public long checkpointIntervalMs = 60000;
    public boolean stateBloat = false;
    public boolean webhookEnabled = false;
    public String webhookUrl = "https://webhook.site/a1b731f8-6003-41f0-948a-6dd9c8c3fa3f";
}
