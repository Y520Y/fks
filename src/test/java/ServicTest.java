import com.service.LogInfoService;
import org.junit.Test;

import javax.annotation.Resource;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * description
 *
 * @author wdj on 2018/6/10
 */
public class ServicTest extends BaseJunit4Test{


    @Resource
    LogInfoService logInfoService;

    @Test
    public void testLogService(){
        List<Map<String,Object>> list = new ArrayList<>();
        Map<String,Object> map1 = new HashMap();
        map1.put("asset_id","11");
        map1.put("PROVINCE_CODE","12221");
        map1.put("CITY_CODE","22");
        map1.put("PV",123);
        list.add(map1);
        Map<String,Object> map2 = new HashMap();
        map2.put("asset_id","22");
        map2.put("province_code","code2");
        map2.put("province_code1","code221");
        list.add(map2);
        logInfoService.save("zlk_asset_ip",list);

    }
}
