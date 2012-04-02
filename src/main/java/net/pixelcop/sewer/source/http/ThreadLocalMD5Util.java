package net.pixelcop.sewer.source.http;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import org.apache.commons.codec.binary.Hex;
import org.apache.commons.codec.binary.StringUtils;

/**
 * Avoid creating new MessageDigest objects on every request
 *
 * @author chetan
 *
 */
public class ThreadLocalMD5Util {

    private static final String MD5 = "MD5";

    private static ThreadLocal<MessageDigest> digester = new ThreadLocal<MessageDigest>() {
      @Override
        protected MessageDigest initialValue() {
          try {
              return MessageDigest.getInstance(MD5);
          } catch (NoSuchAlgorithmException e) {
              throw new RuntimeException(e.getMessage());
          }
        }
    };

    public static String md5Hex(String data) {
        return Hex.encodeHexString(digester.get().digest(StringUtils.getBytesUtf8(data)));
    }

}
