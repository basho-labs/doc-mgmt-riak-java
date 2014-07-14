package com.basho.proserv.documentstore;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;

import org.apache.commons.io.IOUtils;
import org.apache.tika.config.TikaConfig;
import org.apache.tika.exception.TikaException;
import org.apache.tika.io.TikaInputStream;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.mime.MediaType;
import org.xml.sax.SAXException;

public class MimeTypeUtility
{
  public static String getRawMimeType(InputStream stream, String fileName)
          throws IOException, SAXException, TikaException
  {
    byte[] fileContents = IOUtils.toByteArray(stream);
    return getRawMimeType(fileContents, fileName);
  }

  public static String getRawMimeType(byte[] fileContents, String fileName)
          throws IOException, SAXException, TikaException
  {
    TikaConfig tika = new TikaConfig();
    Metadata metadata = new Metadata();
    metadata.set(Metadata.RESOURCE_NAME_KEY, fileName);
    MediaType mediaType = tika.getDetector().detect(
            TikaInputStream.get(new ByteArrayInputStream(fileContents)), metadata);

    String mimeType = mediaType.getType() + "/" + mediaType.getSubtype();
    return mimeType;
  }
}
