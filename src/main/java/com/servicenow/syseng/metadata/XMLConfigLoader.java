package com.servicenow.syseng.metadata;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.util.Properties;

/**
 * Created with IntelliJ IDEA.
 * User: SERVICE-NOW\eason.hu
 * Date: 4/24/13
 * Time: 5:19 PM
 * To change this template use File | Settings | File Templates.
 */
public class XMLConfigLoader {
    public static final String CONFIG_PATH = "src/main/config.xml";
    private static Properties config = new Properties();

    public XMLConfigLoader() throws Exception {
        init();
    }

    public static void init() throws Exception {
        loadConfig();
    }

    public static void loadConfig() throws Exception {
        FileInputStream fis = new FileInputStream(CONFIG_PATH);
        config.loadFromXML(fis);
    }

    public static void saveConfig() throws Exception {
        FileOutputStream fos = new FileOutputStream(CONFIG_PATH);
        config.storeToXML(fos, null);
    }

    public static String get(String key) {
        return config.getProperty(key);
    }

    public static void set(String key, String value) {
        config.setProperty(key, value);
    }
}
