package io.druid.server.lookup;

import com.metamx.common.lifecycle.Lifecycle;
import io.druid.server.lookup.namespace.cache.OnHeapNamespaceExtractionCacheManager;

import java.util.concurrent.ConcurrentMap;

public class OnHeapTest {
    public static void main(String[] args) {
        Lifecycle lifecycle = new Lifecycle();
        OnHeapNamespaceExtractionCacheManager manager = new OnHeapNamespaceExtractionCacheManager(lifecycle, null, null);

        ConcurrentMap<String, String> map = manager.getCacheMap("test");
        map.put("asd","haha");

        System.out.println(manager.getCacheMap("test").get("asd"));
    }
}
