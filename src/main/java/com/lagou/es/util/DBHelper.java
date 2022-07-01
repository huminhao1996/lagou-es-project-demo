package com.lagou.es.util;

import java.sql.Connection;
import java.sql.DriverManager;

public class DBHelper {
    public static final String url = "";
    public static final String name = "com.mysql.cj.jdbc.Driver";
    public static final String user = "";
    public static final String password = "";
    private  static Connection  connection = null;

    public  static   Connection  getConn(){
        try {
            Class.forName(name);
            connection = DriverManager.getConnection(url,user,password);
        }catch (Exception e){
            e.printStackTrace();
        }
        return  connection;
    }
}
