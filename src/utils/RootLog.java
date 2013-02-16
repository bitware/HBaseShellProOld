package utils;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.log4j.PropertyConfigurator;

public class RootLog {
    private static final String LOG4J_CONF_FILE = "./conf/log4j.properties";

    public static String logDir = "logs";   // default "logs"

    // static initializer
    static {
        init();
    }

    private static void init() {
        Properties properties = new Properties();

        try {
            properties.load(new FileInputStream(LOG4J_CONF_FILE));
        } catch (IOException e) {
            getLog().error(null, e);
        }

        // configure log4j
        PropertyConfigurator.configure(properties);

        // property "log.dir"
        logDir = properties.getProperty("log.dir", logDir);
    }

    public static Log getLog() {
        StackTraceElement callerInfo = Utils.getCallerInfo();
        String            className  = callerInfo.getClassName();
        return LogFactory.getLog(className);
    }
}
