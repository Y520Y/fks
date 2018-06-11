package com.myflume;

import com.common.SpringContextHolder;
import com.service.LogInfoService;
import net.sf.json.JSONObject;
import org.apache.commons.lang.StringUtils;
import org.apache.flume.*;
import org.apache.flume.conf.Configurable;
import org.apache.flume.sink.AbstractSink;
import org.apache.tomcat.jdbc.pool.DataSource;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * 自定义sink
 *
 * @author wdj on 2018/6/8
 */
public class MySink extends AbstractSink implements Configurable{

    private String jdbcurl;
    private String username;
    private String password;
    private Integer batchSize;
    private String tablename;
    private DataSource dataSource;
    // 获取flume的配置参数
    @Override
    public void configure(Context context) {
        jdbcurl=context.getString("jdbc_url");
        username=context.getString("jdbc_username");
        password=context.getString("jdbc_password");
        batchSize = context.getInteger("jdbc_batchsize",1000);
        tablename = context.getString("tablename");
    }

    // Initialize the connection to the external repository (e.g. HDFS) that
    // this Sink will forward Events to
    @Override
    public synchronized void start() {
        if(!StringUtils.isBlank(jdbcurl) && !StringUtils.isBlank(username) && !StringUtils.isBlank(password)){
            dataSource = new DataSource();
            dataSource.setUrl(jdbcurl);
            dataSource.setUsername(username);
            dataSource.setPassword(password);
            dataSource.setInitialSize(5);
            dataSource.setMaxActive(20);
            dataSource.setMinIdle(5);
            dataSource.setMaxIdle(20);
            dataSource.setMaxWait(30000);
        }
    }

    // Disconnect from the external respository and do any
    // additional cleanup
    @Override
    public synchronized void stop() {
        dataSource.close();
        super.stop();
    }

    // log日志数据格式需要相同，具体格式以第一个为准
    @Override
    public Status process() throws EventDeliveryException {
        LogInfoService logInfoService = SpringContextHolder.getBean("logInfoService");
        Status status = null;
        // Start transaction

        Channel ch = getChannel();
        Transaction txn = ch.getTransaction();
        txn.begin();
        try {
            // This try clause includes whatever Channel operations you want to do
            List<Map<String,Object>> datas = new ArrayList<>();
            long processedEvent = 0;
            for (; processedEvent < batchSize; processedEvent++) {
                Event event = ch.take();
                byte[] eventBody;
                if(event != null){
                    eventBody = event.getBody();
                    String line= new String(eventBody,"UTF-8");
                    if (line.length() > 0 ){
                        int start = line.indexOf('{');
                        int end = line.lastIndexOf('}');
                        if(start != -1 && end!= -1){
                            String dataStr = line.substring(start,end+1);
                            Map<String,Object> map = JSONObject.fromObject(dataStr);
                            datas.add(map);
                        }
                    }
                }else{
                    status = Status.BACKOFF;
                    break;
                }
            }
            if(!StringUtils.isBlank(tablename) && datas!=null && !datas.isEmpty()){
                logInfoService.save(tablename,datas);
                status = Status.READY;
            }
            txn.commit();
        } catch (Throwable t) {

            txn.rollback();
            // Log exception, handle individual exceptions as needed

            status = Status.BACKOFF;
            // re-throw all Errors
            if (t instanceof Error) {
                System.out.println("[ERROR][process]"+ t.toString());
                throw (Error)t;
            }
        }
        return status;
    }

}
