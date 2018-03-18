package mintaka;

import mintaka.commons.MintakaException;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.net.URL;

/**
 * 读取配置文件的reader
 * <p>
 * Created by qinjiazheng on 2017/12/1.
 */
public class LoadFileUtil {
    private static final Logger LOGGER = LoggerFactory.getLogger(LoadFileUtil.class);

    /**
     * Get input stream input stream.
     *
     * @param filePath the file path
     * @return the input stream
     * @throws MintakaException the mintaka exception
     */
    public static InputStream getInputStream(String filePath) throws MintakaException {
        if (StringUtils.isBlank(filePath)) {
            LOGGER.error("The file path can not be null");
            throw new MintakaException("The file path can not be null");
        }
        try {
            ClassLoader classLoader = LoadFileUtil.class.getClassLoader();
//            System.out.println("filePath is ---------->"+filePath);
            URL url = classLoader.getResource(filePath);
            return url.openStream();
        } catch (Exception e) {
            LOGGER.error("Can not open the InputStream for file '{}'", filePath, e);
            throw new MintakaException("Can not open the InputStream for " + filePath);
        }
    }
}
