package org.apache.rados;

import java.io.File;
import java.io.InputStream;
import java.security.MessageDigest;

import org.apache.commons.codec.binary.Hex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ceph.rados.IoCTX;
import com.ceph.rados.Rados;
import com.ceph.rados.exceptions.RadosException;

/**
 * Created by arno.broekhof on 1/6/16.
 */
public class RadosConnection {

    protected static final Logger logger = LoggerFactory.getLogger(RadosConnection.class);
    private static Rados rados;
    private static IoCTX ioCTX;

    private static String ENV_CONFIG_FILE = System.getenv("RADOS_JAVA_CONFIG_FILE");
    private static String ENV_ID = System.getenv("RADOS_JAVA_ID");
    private static String ENV_POOL = System.getenv("RADOS_JAVA_POOL");

    private String CONFIG_FILE = ENV_CONFIG_FILE == null ? "/etc/ceph/ceph.conf" : ENV_CONFIG_FILE;
    private String ID = ENV_ID == null ? "admin" : ENV_ID;
    private String POOL = ENV_POOL == null ? "data" : ENV_POOL;

    public RadosConnection(final String CONFIG_FILE, final String ID, final String POOL) {

        this.CONFIG_FILE = CONFIG_FILE;
        logger.info("ceph config file set to {}", CONFIG_FILE);

        this.ID = ID;
        logger.info("ceph id set to {}", ID);

        this.POOL = POOL;
        logger.info("ceph pool set to {}", POOL);

        try {
            this.init();
        }
        catch (RadosException e) {
            throw new IllegalStateException(e.getMessage());
        }
    }

    /**
     * Getter for the Rados object
     *
     * @return
     */
    public Rados getRados() {
        return rados;
    }

    /**
     * Initialize connection
     */
    public void init() throws RadosException {
        rados = new Rados(ID);
        rados.confReadFile(new File(CONFIG_FILE));
        rados.connect();
        ioCTX = rados.ioCtxCreate(POOL);
        logger.info("Connected to Ceph cluster: {} ", rados.clusterFsid());
    }

    /**
     * Put object from inputstream to rados with MD5 calculation
     *
     * @param inputStream the to read from.
     * @param objectName  the name of the object to insert.
     * @param bufferSize  the buffer size to use for reading the inputstream
     * @throws Exception
     */
    public void putObject(final InputStream inputStream, final String objectName, final int bufferSize) throws Exception {
        int length = inputStream.available();
        byte[] buffer = new byte[bufferSize];
        int bytesRead;
        int offset = 0;
        logger.info("Copying file with objectName: {}", objectName);
        MessageDigest md = MessageDigest.getInstance("MD5");

        while ((bytesRead = inputStream.read(buffer)) > 0) {
            md.update(buffer, 0, bytesRead);
            logger.debug("Bytes read: {} offset: {} length: {}", bytesRead, offset, (length - offset));
            ioCTX.truncate(objectName, offset);
            ioCTX.write(objectName, buffer, offset);
            offset += bytesRead;
        }
        byte[] digest = md.digest();
        ioCTX.setExtentedAttribute(objectName, "MD5", new String(Hex.encodeHex(digest)));
    }
}
