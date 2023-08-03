package io.pixelsdb.pixels.worker.common;

import com.google.common.collect.ImmutableList;
import io.pixelsdb.pixels.common.physical.Storage;
import io.pixelsdb.pixels.common.physical.StorageFactory;
import io.pixelsdb.pixels.core.PixelsReader;
import io.pixelsdb.pixels.core.PixelsReaderImpl;
import io.pixelsdb.pixels.core.reader.PixelsReaderOption;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

import static io.pixelsdb.pixels.storage.s3.Minio.ConfigMinio;

public class TestHttpServerClient {

    @Test
    public void testServer() throws IOException {
        ConfigMinio("dummy-region", "http://localhost:9000", "5hBbowVNnecVfpOXikrw", "oRB8qTbvsYcmKotnFhECjlJulCOzJSoVmbo72IyU");
        Storage minio = StorageFactory.Instance().getStorage(Storage.Scheme.minio);
        PixelsReaderImpl.Builder builder = PixelsReaderImpl.newBuilder()
                .setStorage(minio)
                .setPath("pixels-tpch/")
                .setEnableCache(false)
                .setCacheOrder(ImmutableList.of())
                .setPixelsCacheReader(null)
                .setPixelsFooterCache(null);
        PixelsReader pixelsReader = builder.build();

        List<String> files = minio.listPaths(".");
        for (String file : files)
        {
            System.out.println(file);
        }

//        PixelsReaderOption option = new PixelsReaderOption();
//        option.skipCorruptRecords(true);
//        option.tolerantSchemaEvolution(true);
//        pixelsReader.read(option);
//
//        io.pixelsdb.pixels.common.utils.HttpServer h;

    }

    @Test
    public void testClient() {

    }
}
