package cn.edu.zju.cheetah.jdbc;

import com.google.common.base.Function;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.Maps;
import org.junit.Test;

import javax.annotation.Nullable;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Created by edwardlol on 17-3-14.
 */
public class UtilTests {

    @Test
    public void sbLenTest() {
        StringBuilder sb = new StringBuilder();

        for (int i = 0; i < 10; i++) {
            sb.append(i).append(',').append(' ');
        }

        sb.setLength(28);
        System.out.println(sb.toString());
    }

    @Test
    public void asMapTest() {
        Set<Integer> intSet = new HashSet<>();
        intSet.add(1);
        intSet.add(2);
        intSet.add(3);

        Map<Integer, Integer> intMap = Maps.asMap(intSet, new Function<Integer, Integer>() {
            @Nullable
            @Override
            public Integer apply(@Nullable Integer input) {
                if (input != null) {
                    return input * 2;
                } else {
                    return null;
                }
            }
        });

        for (Map.Entry<Integer, Integer> entry : intMap.entrySet()) {
            System.out.println("orig: " + entry.getKey() + "; after: " + entry.getValue());
        }
    }

    @Test
    public void loadingCacheTest() throws Exception {
        LoadingCache<String, String> cahceBuilder = CacheBuilder.newBuilder()
                .build(new CacheLoader<String, String>() {
                    @Override
                    public String load(String key) throws Exception {
                        return "hello " + key + "!";
                    }
                });

        System.out.println("jerry value:" + cahceBuilder.get("jerry"));
        System.out.println("jerry value:" + cahceBuilder.get("jerry"));
        System.out.println("peida value:" + cahceBuilder.get("peida"));
        System.out.println("peida value:" + cahceBuilder.get("peida"));
        System.out.println("lisa value:" + cahceBuilder.get("lisa"));
        System.out.println("harry value:" + cahceBuilder.get("harry"));
        // put the content into cache
        cahceBuilder.put("harry", "ssdded");
        System.out.println("harry value:" + cahceBuilder.get("harry"));
    }
}
