package com.basho.proserv.documentstore;

import java.io.IOException;
import java.io.InputStream;

import org.apache.commons.io.IOUtils;
import org.apache.tika.exception.TikaException;
import org.junit.Test;
import org.xml.sax.SAXException;

import static org.junit.Assert.assertEquals;

public class MimeTypeUtilityTest
{
  @Test
  public void testPDFContentType() throws IOException, SAXException, TikaException
  {
    try (InputStream fileStream = getClass().getResourceAsStream("/testpdf.pdf"))
    {
      assertEquals("application/pdf", MimeTypeUtility.getRawMimeType(fileStream, "foo"));
    }
    try (InputStream fileStream = getClass().getResourceAsStream("/testpdf.pdf"))
    {
      byte[] fileContents = IOUtils.toByteArray(fileStream);
      assertEquals("application/pdf", MimeTypeUtility.getRawMimeType(fileContents, "foo"));
    }
  }
}
