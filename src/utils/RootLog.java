package utils;

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
        Properties properties = PropertiesHelper.getPropertiesBase(LOG4J_CONF_FILE);

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
