package com.dao;

import org.apache.ibatis.annotations.Param;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Map;

/**
 * description
 *
 * @author wdj on 2018/6/9
 */
@Repository
public interface LogInfoDao {

    List<Map<String,String>> getColumn(@Param("tablename") String tablename);

    void save(@Param("tablename") String tablename,@Param("data")List<Map> data);
}
