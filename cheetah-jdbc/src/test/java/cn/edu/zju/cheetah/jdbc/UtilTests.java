package cn.edu.zju.cheetah.jdbc;

import org.junit.Test;

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
}
