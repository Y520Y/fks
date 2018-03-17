import org.junit.Test;

import java.util.UUID;

/**
 * description
 *
 * @author wdj on 2018/3/13
 */
public class CommonTest {


    @Test
    public void testUUID(){
        System.out.println(UUID.randomUUID().toString());
    }

    @Test
    public void testSub(){
        System.out.println("12,23,4454".substring(0,"12,23,4454".lastIndexOf(",")));
    }
}
