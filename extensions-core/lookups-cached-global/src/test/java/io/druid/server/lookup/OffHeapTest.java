package io.druid.server.lookup;

import com.metamx.common.lifecycle.Lifecycle;
import io.druid.server.lookup.namespace.cache.OffHeapNamespaceExtractionCacheManager;

import java.util.concurrent.ConcurrentMap;

public class OffHeapTest {
    public static void main(String[] args) throws InterruptedException {
        Lifecycle lifecycle = new Lifecycle();
        OffHeapNamespaceExtractionCacheManager manager = new OffHeapNamespaceExtractionCacheManager(lifecycle, null, null
        );

        ConcurrentMap<String, String> map = manager.getCacheMap("test");
        map.put("asd","haha");

        System.out.println(manager.getCacheMap("test").get("asd"));

        ConcurrentMap<String, String> map2 = manager.getCacheMap("test");
        map2.put("asd2", "haha2");

        System.out.println(manager.getCacheMap("test").get("asd2"));
        System.out.println(manager.getCacheMap("test").get("asd"));
    }
}
