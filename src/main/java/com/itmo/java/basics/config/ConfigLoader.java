package com.itmo.java.basics.config;

import com.itmo.java.basics.console.DatabaseCommand;

import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Properties;
import java.util.ResourceBundle;

/**
 * Класс, отвечающий за подгрузку данных из конфигурационного файла формата .properties
 */
public class ConfigLoader {
    public static final String KVS_PORT = "kvs.port";
    private final String name;

    /**
     * По умолчанию читает из server.properties
     */
    public ConfigLoader() {
        this.name = "server.properties";
    }

    /**
     * @param name Имя конфикурационного файла, откуда читать
     */
    public ConfigLoader(String name) {
        this.name = name;
    }

    /**
     * Считывает конфиг из указанного в конструкторе файла.
     * Если не удалось считать из заданного файла, или какого-то конкретно значения не оказалось,
     * то используют дефолтные значения из {@link DatabaseConfig} и {@link ServerConfig}
     * <br/>
     * Читаются: "kvs.workingPath", "kvs.host", "kvs.port" (но в конфигурационном файле допустимы и другие проперти)
     */
    public DatabaseServerConfig readConfig() {

        Properties properties = new Properties();
        try (FileInputStream fileInputStream = new FileInputStream(name)){
            properties.load(fileInputStream);
        } catch (Exception e) {
            e.printStackTrace();
        }
        try (InputStream is = this.getClass().getClassLoader().getResourceAsStream(name)){
            properties.load(is);
        } catch (Exception e) {
            e.printStackTrace();
        }
        String hostName = properties.getProperty("kvs.host", ServerConfig.DEFAULT_HOST);
        String workingPath = properties.getProperty("kvs.workingPath", DatabaseConfig.DEFAULT_WORKING_PATH);
        String portName = properties.getProperty("kvs.port", String.valueOf(ServerConfig.DEFAULT_PORT));

        ServerConfig serverConfig = new ServerConfig(hostName, Integer.parseInt(portName));
        DatabaseConfig databaseConfig = new DatabaseConfig(workingPath);
        DatabaseServerConfig databaseServerConfig = new DatabaseServerConfig(serverConfig, databaseConfig);
        return databaseServerConfig;
    }
}
