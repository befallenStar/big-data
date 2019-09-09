package com.junenatte.hadoop.weblogs;

import java.util.HashSet;
import java.util.Set;

public class WebLog {
    private String remoteAddr;
    private String remoteUser;
    private String timeLocal;
    private String request;
    private String status;
    private String bodyBytesSent;
    private boolean valid = true;

    public static WebLog parse(String line) {
        WebLog wl = new WebLog();
        String[] sp = line.split(" ");
        if (sp.length > 9) {
            wl.setRemoteAddr(sp[0]);
            wl.setRemoteUser(sp[1]);
            String timeLocal = sp[3].substring(1);
            timeLocal = timeLocal.replace('[', '/').replace('/', '-').replace(':', '-');
            wl.setTimeLocal(timeLocal);
            String req=sp[6];
            if (req != null && req.length() > 1) {
                if (req.lastIndexOf('/') != 0 && req.lastIndexOf('/') > 0)
                    req = req.substring(0, req.indexOf('/', 1));
                if (req.indexOf('/') == 0 && req.indexOf('?') > 0)
                    req = req.substring(0, req.indexOf('?', 1));
                if (req.indexOf('/') == 0 && req.indexOf(';') > 0)
                    req = req.substring(0, req.indexOf(';', 1));
            }
            wl.setRequest(req);
            wl.setStatus(sp[8]);
            wl.setBodyBytesSent(sp[9]);
            if (Integer.parseInt(wl.getStatus()) >= 400)
                wl.setValid(false);
        } else
            wl.setValid(false);
        return wl;
    }

    public static WebLog filterData(String line) {
        WebLog wl = WebLog.parse(line);
        Set<String> pages = new HashSet<>();
        pages.add("/message");
        pages.add("/register");
        pages.add("/captcha");
        pages.add("/commentEvent");
        pages.add("/m/special");
        pages.add("/login");
        pages.add("/song");
        pages.add("/mention");
        pages.add("/share");
        pages.add("/space");
        pages.add("/index");
        if (!"/".equals(wl.getRequest()) && !pages.contains(wl.getRequest()))
            wl.setValid(false);
        return wl;
    }

    @Override
    public int hashCode() {
        return remoteAddr.hashCode() + remoteUser.hashCode() + timeLocal.hashCode() + request.hashCode() + status.hashCode() + bodyBytesSent.hashCode();
    }

    @Override
    public String toString() {
        return "[remoteAddr]\t" + remoteAddr + '\n' +
                "[remoteUser]\t" + remoteUser + '\n' +
                "[timeLocal]\t" + timeLocal + '\n' +
                "[request]\t" + request + '\n' +
                "[status]\t" + status + '\n' +
                "[bodyBytesSent]\t" + bodyBytesSent + '\n' +
                "[valid]\t" + valid + '\n';
    }

    public WebLog() {
    }

    public WebLog(String remoteAddr, String remoteUser, String timeLocal, String request, String status, String bodyBytesSent, boolean valid) {
        this.remoteAddr = remoteAddr;
        this.remoteUser = remoteUser;
        this.timeLocal = timeLocal;
        this.request = request;
        this.status = status;
        this.bodyBytesSent = bodyBytesSent;
        this.valid = valid;
    }

    public String getRemoteAddr() {
        return remoteAddr;
    }

    public void setRemoteAddr(String remoteAddr) {
        this.remoteAddr = remoteAddr;
    }

    public String getRemoteUser() {
        return remoteUser;
    }

    public void setRemoteUser(String remoteUser) {
        this.remoteUser = remoteUser;
    }

    public String getTimeLocal() {
        return timeLocal;
    }

    public void setTimeLocal(String timeLocal) {
        this.timeLocal = timeLocal;
    }

    public String getRequest() {
        return request;
    }

    public void setRequest(String request) {
        this.request = request;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public String getBodyBytesSent() {
        return bodyBytesSent;
    }

    public void setBodyBytesSent(String bodyBytesSent) {
        this.bodyBytesSent = bodyBytesSent;
    }

    public boolean isValid() {
        return valid;
    }

    public void setValid(boolean valid) {
        this.valid = valid;
    }

    public static void main(String[] args) {
        /*
        [remoteAddr]	120.83.228.7
        [remoteUser]	-
        [timeLocal]	14/Sep[2017:00:00:01
        [request]	/message/getNoReadCount?_=121212
        [status]	200
        [bodyBytesSent]	117
        [valid]	true
        */
        String line = "120.83.228.7 - - [14/Sep[2017:00:00:01 +0800] \"GET /asd HTTP/1.1\" 200 117";
        System.out.println(WebLog.filterData(line).toString());
    }

}
