package com.myflume;

import com.common.SpringContextHolder;
import com.service.LogInfoService;
import net.sf.json.JSONObject;
import org.apache.commons.lang.StringUtils;
import org.apache.flume.*;
import org.apache.flume.conf.Configurable;
import org.apache.flume.sink.AbstractSink;
import org.apache.tomcat.jdbc.pool.DataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * 自定义sink
 *
 * @author wdj on 2018/6/8
 */
public class OracleSink extends AbstractSink implements Configurable{

    private Logger logger = LoggerFactory.getLogger(getClass());

    private Integer tryCount = 0;

    //MAX_TRY_COUNT 次尝试提交之后若数据个数还未达到batchSize，则试着提交
    private final Integer MAX_TRY_COUNT = 2;

    private String jdbcurl;
    private String username;
    private String password;
    private Integer batchSize;
    private String tablename;
    private DataSource dataSource;
    LogInfoService logInfoService;
    private List<Map<String,Object>> datas = new ArrayList<>();

    // 获取flume的配置参数
    @Override
    public void configure(Context context) {
        ClassPathXmlApplicationContext applicationContext = new ClassPathXmlApplicationContext(
                new String[] { "classpath:spring-context.xml" });
        applicationContext.start();
        logInfoService = SpringContextHolder.getBean("logInfoService");
        dataSource = SpringContextHolder.getBean("dataSource");
        jdbcurl=context.getString("jdbc_url");
        username=context.getString("jdbc_username");
        password=context.getString("jdbc_password");
        batchSize = context.getInteger("jdbc_batchsize",10);
        tablename = context.getString("jdbc_tablename");
        logger.info("初始化数据 ==== tablename:"+tablename+";jdbcurl："+jdbcurl+";username:"+username+";batchSize"+batchSize);
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
        logger.info("sink关闭。。。。。。。。保存缓存中的剩余数据");
        if(datas != null && !datas.isEmpty()){
            logInfoService.save(tablename,datas);
            logger.info("提交"+datas.size()+"条数据");
        }
        dataSource.close();
        super.stop();
    }

    @Override
    public Status process() throws EventDeliveryException {
        Status status = null;
        // Start transaction
        Channel ch = getChannel();
        Transaction txn = ch.getTransaction();
        txn.begin();
        try {
            if(StringUtils.isBlank(tablename)){
                throw new Exception("tablename不能为空！");
            }
            // This try clause includes whatever Channel operations you want to do
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
                    logger.info("even为空，回退。。。");
                    status = Status.BACKOFF;
                    break;
                }
            }
            boolean canCommit = (status != Status.BACKOFF && datas!=null && !datas.isEmpty())
                    || (tryCount >= MAX_TRY_COUNT && datas!=null && !datas.isEmpty());
            // 将数据复制到临时变量，将data去空，当时若flume在datas浮空后未保存数据就关闭，则还是会丢失一部分数据
            List<Map<String,Object>> tem = new ArrayList<>();
            tem.addAll(datas);
            datas = new ArrayList<>();
            if(canCommit){
                logInfoService.save(tablename,tem);
                logger.info("提交"+datas.size()+"条数据");
                status = Status.READY;
                tryCount=0;
                txn.commit();
            }else if(status == Status.BACKOFF){
                txn.rollback();
                tryCount++;
            }else{
                logger.info("数据为空！");
                status = Status.BACKOFF;
                txn.rollback();
                tryCount=0;
            }
        } catch (Exception e) {
            txn.rollback();
            // Log exception, handle individual exceptions as needed
            logger.error("保存数据出错：",e);
            status = Status.BACKOFF;
        }
        txn.close();
        return status;
    }

    public static void main(String[] args){
        OracleSink oracleSink = new OracleSink();
        oracleSink.configure(null);
        oracleSink.start();
        try {
            oracleSink.process();
        } catch (EventDeliveryException e) {
            e.printStackTrace();
        }

    }
}
