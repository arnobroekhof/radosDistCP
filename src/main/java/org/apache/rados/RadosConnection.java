package org.apache.rados;

import com.ceph.rados.IoCTX;
import com.ceph.rados.Rados;
import com.ceph.rados.exceptions.RadosException;
import com.ceph.rados.jna.RadosObjectInfo;
import org.apache.commons.codec.binary.Hex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.InputStream;
import java.security.MessageDigest;

/**
 * Created by arno.broekhof on 1/6/16.
 */
public class RadosConnection {

    protected static final Logger logger = LoggerFactory.getLogger(RadosConnection.class);
    private static Rados rados;
    private static IoCTX ioCTX;


    private String cephConfigFile;
    private String cephId;
    private String cephPool;

    public RadosConnection(final String cephConfigFile, final String cephId, final String cephPool) {

        this.cephConfigFile = cephConfigFile;
        logger.info("ceph config file set to {}", cephConfigFile);

        this.cephId = cephId;
        logger.info("ceph id set to {}", cephId);

        this.cephPool = cephPool;
        logger.info("ceph pool set to {}", cephPool);

        try {
            this.init();
        } catch (RadosException e) {
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
        rados = new Rados(cephId);
        rados.confReadFile(new File(cephConfigFile));
        rados.connect();
        ioCTX = rados.ioCtxCreate(cephPool);
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

    /**
     * Validate the object based on name and size.
     *
     * @param objectName
     * @param objectSize
     * @return true if the object is present and the given size matches the given objectSize.
     */
    public boolean validate(final String objectName, final long objectSize) {
        boolean _return = false;
        if (this.exists(objectName).getSize() == objectSize) {
            _return = true;
        }
        return _return;
    }

    /**
     * Validate object based on name and md5sum.
     * NOTE: this function assumes that there is an extended attribute called MD5.
     *
     * @param objectName
     * @param md5
     * @return true if the object is present and the md5 sum matches the given md5.
     */
    public boolean validate(final String objectName, final String md5) {
        boolean _return = false;
        if (this.exists(objectName).getOid().equals(objectName)) {
            try {
                if (ioCTX.getExtentedAttribute(objectName, "MD5").equals(md5)) {
                    _return = true;
                }
            } catch (RadosException e) {
                logger.error(e.getMessage());
            }
        }
        return _return;
    }

    /**
     * Check if an object with the givven objectName is present.
     *
     * @param objectName
     * @return if the object name is present and equals the given objectName then it
     * return the RadosObjectInfo.
     */
    private RadosObjectInfo exists(final String objectName) {
        RadosObjectInfo radosObjectInfo = null;
        try {
            radosObjectInfo = ioCTX.stat(objectName);
            if (radosObjectInfo.getOid().equals(objectName)) {
                return radosObjectInfo;
            }
        } catch (RadosException e) {
            logger.error(e.getMessage());
        }
        return radosObjectInfo;
    }
}
