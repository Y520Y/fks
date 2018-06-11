package com.service;

import com.dao.LogInfoDao;
import com.entity.ColumnDataBean;
import org.apache.commons.lang.StringUtils;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.util.*;

/**
 * description
 *
 * @author wdj on 2018/6/9
 */
@Service
public class LogInfoService {

    @Resource
    LogInfoDao logInfoDao;

    public void save(String tablename,List<Map<String,Object>> datas){
        //除了id所有列
        List<Map<String,String>> columnList = logInfoDao.getColumn(tablename.toUpperCase());
        //使用linkedHashMap保存原有的顺序
        Map<String,String> columns = new LinkedHashMap();
        for (Map<String, String> stringStringMap : columnList) {
            columns.put(stringStringMap.get("COLUMN_NAME"),getJdbcType(stringStringMap.get("DATA_TYPE")));
        }
        List<Map> dataMap = new ArrayList<>();
        for (Map<String, Object> data : datas) {
            data =transformUpperCase(data);
            Map map = new LinkedHashMap();
            for (String s : columns.keySet()) {
                ColumnDataBean dataBean = new ColumnDataBean();
                dataBean.setValue(data.get(s));
                dataBean.setType(columns.get(s));
                //保存字段值，及字段类型
                map.put(s,dataBean);
            }
            dataMap.add(map);
        }
        logInfoDao.save(tablename,dataMap);
    }

    public Map<String, Object> transformUpperCase(Map<String, Object> orgMap) {
        Map<String, Object> resultMap = new HashMap<>();

        if (orgMap == null || orgMap.isEmpty()) {
            return resultMap;
        }

        Set<String> keySet = orgMap.keySet();
        for (String key : keySet) {
            String newKey = key.toUpperCase();

            resultMap.put(newKey, orgMap.get(key));
        }
        return resultMap;
    }

    public String getJdbcType(String dataSourceType){
        if(StringUtils.isBlank(dataSourceType)){
            return "VARCHAR";//默认字符串
        }else if(dataSourceType.indexOf("TIMESTAMP")>-1){
            return "TIMESTAMP";
        }else if(dataSourceType.indexOf("CHAR")>-1){
            return "VARCHAR";
        }else if(dataSourceType.indexOf("NUMBER")>-1){
            return "NUMERIC";
        }else{
            return "VARCHAR";
        }
    }

}
