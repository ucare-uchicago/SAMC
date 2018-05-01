package mc.zookeeper;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Properties;

public class ZKAspectProperties {
    
    public static final String INTERCEPTOR_NAME = "mc_name";
    public static final String LE_CHECK = "le_check";
    public static final String ZAB_CHECK = "zab_check";
    
    private static Properties prop;
    
    static {
        prop = new Properties();
        try {
            String configFilePath = System.getenv("MC_CONFIG");
            FileInputStream configInputStream = new FileInputStream(configFilePath);
            prop = new Properties();
            prop.load(configInputStream);
            configInputStream.close();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    
    public static String getConfig(String key) {
        return prop.getProperty(key);
    }
    
    public static String getInterceptorName() {
        return prop.getProperty(INTERCEPTOR_NAME);
    }
    
    public static boolean isLeChecked() {
        return Boolean.parseBoolean(prop.getProperty(LE_CHECK));
    }

    public static boolean isZabChecked() {
        return Boolean.parseBoolean(prop.getProperty(ZAB_CHECK));
    }

}
