package com.nio.map.job;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.JSONPath;
import com.alibaba.fastjson.parser.Feature;
import com.nio.map.bean.MapLinkRaw;
import com.nio.map.bean.NewResponse;
import com.nio.map.bean.SnapshotSecond;
import com.nio.map.util.ObjectUtil;
import com.nio.map.util.StringUtil;
import com.sun.org.slf4j.internal.Logger;
import com.sun.org.slf4j.internal.LoggerFactory;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.*;

/**
 * Created by rain.chen on 2023/3/27
 **/
public class DwdMapLinkJob implements Serializable {
    private Map<String, String> map;

    private static final Logger LOG = LoggerFactory.getLogger(DwdMapLinkJob.class);

    int disableDecimalFeature = JSON.DEFAULT_PARSER_FEATURE & ~Feature.UseBigDecimal.getMask();


    public static void main(String[] args) {
        DwdMapLinkJob dwdMapLinkJob = new DwdMapLinkJob();

//        dwdMapLinkJob.getStringMap(args);

        dwdMapLinkJob.run();

    }

    /**
     * 处理外部参数
     * @param args cycle-1d、cycle
     */
    //--name insert_dwd_ad_snapshot_driving_spark_di_${cycle-1d} --class com.nio.driving.job.DwdSnapshotMsJob spark-snapshot-etl.jar  cycle-1d:${cycle-1d},cycle:${cycle}
    private void getStringMap(String[] args){
        String para = args[0];
//        System.out.println(para);//cycle-1d:${cycle-1d},cycle:${cycle}

        String[] splits = para.split(",");
        map = new HashMap<>();
        for (String kv : splits) {
            String[] kvs = kv.split(":");
            System.out.println(kvs[0] + kvs[1]);// map=(cycle-1d, 20230319)、(cycle, 20230320)
            map.put(kvs[0], kvs[1]);
        }
    }

    public void run(){
        //创建SparkSession
        SparkSession ss = SparkSession
                .builder()
                .appName("qq_sd_map_link")
//                .config("hive.exec.dynamic.partition", "true") //动态分区参数
//                .config("hive.exec.dynamic.partition.mode", "nonstrict")
                .config("spark.driver.host","localhost") //k-v
                .master("local[4]")  //本地执行
                .enableHiveSupport() //读orc文件必须hive支持
                .getOrCreate();

        //Source
//        String inputSql = "select vehicle_id\n" +
//                          "\t , ts\n" +
//                          "\t , longitude\n" +
//                          "\t , latitude\n" +
//                          "\t , vehspdkph\n" +
//                          "\t , yaw\n" +
//                          "from ad_d_cdm.dwd_snapshot_second_hi \n" +
//                          "where pt = '${cycle-1h}'";
//
//        //对字符串格式化处理
//        StringUtil.templateStr(inputSql,map);//将${cycle-1h}中的${}去掉，并用map中的key=cycle-1h的value进行替换
//        System.err.println(inputSql);//sout和serr的区别
//        //生成Dataset
//        Dataset<Row> sqlDs = ss.sql(inputSql);


//        ss.read().csv("/Users/rain.chen/IdeaProjects/sd_map_link/data/1-44.csv").show(10);
//        ss.read().orc("/Users/rain.chen/IdeaProjects/sd_map_link/data/000000_0").show(10);
//        ss.read().format("orc").load("/Users/rain.chen/IdeaProjects/sd_map_link/data/000000_0").show(10);

        //最好不要直接在本地连线上数据库，避免搞挂，可以down一份数据集本地测试
        String input = "/Users/rain.chen/IdeaProjects/sd_map_link/data/000000_0 (1)";
        Dataset<Row> orcDs = ss.read().orc(input);
//        orcDs.show(10);

        //Transform
        //1.生成RDD
        JavaRDD<Row> javaRDD = orcDs.javaRDD();
//        List<Row> rowList = javaRDD.take(5);
//        System.out.println(rowList.toString());

        //过滤出单车方便排查
        JavaRDD<Row> filterRDD = javaRDD.filter(new Function<Row, Boolean>() {
            @Override
            public Boolean call(Row row) throws Exception {
                String vid = row.getString(0);
                return vid.equals("8fecbca4b03b46e10802797270001010");
            }
        });
//        System.out.println(filterRDD.collect().toString());

        //2.对RDD进行转换
        //2.1 将vid作为key==》(v1,[v1,t1]), (v1,[v1,t2]), (v1,[v1,t3])
        JavaPairRDD<String, Row> keyByRDD = filterRDD.keyBy(new Function<Row, String>() {
            @Override
            public String call(Row row) throws Exception {
                String vid = row.getString(0);//row.getString(0)即表示vid，类型为String，作为key进行分组
                return vid;
            }
        });
//        System.out.println(keyByRDD.collect().toString());


        //2.2 按vid进行分组==》(v1,[[v1,t1],[v1,t2],[v1,t3]])
        JavaPairRDD<String, Iterable<Row>> groupByKeyRDD = keyByRDD.groupByKey();
//        System.out.println(groupByKeyRDD.collect().toString());

        //2.3 将每一个vid(key)对应的list集合(values)进行循环遍历，按ts排序并过滤静止数据，返回一个新的集合
        JavaPairRDD<String, List<Row>> mapValuesRDD = groupByKeyRDD.mapValues(new Function<Iterable<Row>, List<Row>>() {
            @Override
            public List<Row> call(Iterable<Row> iterable) throws Exception {
                //创建一个List用来存放循环后的数据
                ArrayList<Row> tmpList = new ArrayList<>();
                //生成迭代器进行循环遍历
                Iterator<Row> iterator = iterable.iterator();//Iterable和Iterator的区别
                while (iterator.hasNext()) { //判断当前元素是否存在，不移动指向
                    Row next = iterator.next();//返回当前元素，并指向下一个
                    tmpList.add(next);
                }
                //对集合进行排序，继续List的sort方法重写compare方法
                tmpList.sort(new Comparator<Row>() {
                    @Override
                    public int compare(Row r1, Row r2) {
                        //获取Long类型(10位)的时间戳
                        long t1 = r1.getLong(2);
                        long t2 = r2.getLong(2);
                        //比较大小
                        if (t1 < t2) {
                            return -1;//JDK默认排序为升序，< return -1;> return 1;= return 0;反之降序
                        } else if (t1 > t2) {
                            return 1;
                        }
                        return 0;
                    }
                });
                //
                ArrayList<Row> rowArrayList = new ArrayList<>();
                //将排好序后的集合过滤掉静止(阈值为0)的数据
                for (int i = 0; i < tmpList.size(); i++) {
                    //当前条数据的时间+速度
                    Row r1 = tmpList.get(i);
                    long t1 = r1.getLong(2);
                    float v1 = r1.getFloat(6);

                    if (i == 0){
                        rowArrayList.add(r1);
                    }else {
                        //下一条数据的时间+速度
                        Row r2 = tmpList.get(i-1);
                        long t2 = r2.getLong(2);
                        float v2 = r2.getFloat(6);
                        //时间差*速度>0
                        float odom = (t1 - t2) * v2;
//                        System.out.println("里程差为" + odom);
                        if(odom > 0){
                            rowArrayList.add(r1);
                        }
                    }
                }
//                System.out.println(rowArrayList);
                return rowArrayList;
            }
        });
//        System.out.println(mapValuesRDD.collect().toString());


        //2.4 每辆车的所有轨迹参数全部传入掉用接口
        JavaRDD<Tuple2<String, List<String>>> mapRDD = mapValuesRDD.map(new Function<Tuple2<String, List<Row>>, Tuple2<String, List<String>>>() {
            @Override
            public Tuple2<String, List<String>> call(Tuple2<String, List<Row>> tuple2) throws Exception {
                String vid = tuple2._1();
                List<Row> rows = tuple2._2();
//                System.out.println("rows:"+rows);

                List<List<String>> arrayList = new ArrayList<>();
                MapLinkRaw mapLinkRaw = new MapLinkRaw();

                for (int i = 0; i < rows.size(); i++) {
                    Row row = rows.get(i);
                    long ts = row.getLong(2);
                    double lon = row.getDouble(4);
                    double lat = row.getDouble(5);
                    float spd = row.getFloat(6);
                    float yaw = row.getFloat(7);
                    String track = ts + "," + lon + "," + lat + "," + spd + "," + yaw;
                    List<String> list = Arrays.asList(track.split(","));
//                    System.out.println("list:"+list);
                    arrayList.add(list);
                }
//                System.out.println("lists:"+arrayList);

                List<String> responses = evaluateData(arrayList);
                //异常捕获
                String oldResponse = responses.get(0);
//                System.out.println("oldResponse:" + oldResponse);
                String newResponse = responses.get(1);
//                System.out.println("newResponse:" + newResponse);
                //生成对象
//                mapLinkRaw.setVehicleId(vid);
//                mapLinkRaw.setOldResponse(oldResponse);
//                mapLinkRaw.setNewResponse(newResponse);
//                System.out.println(JSONObject.toJSONString(mapLinkRaw));

                Tuple2<String, List<String>> apply = Tuple2.apply(vid, responses);
                return apply;
//                return mapLinkRaw;
            }
        });

//        mapRDD.collect();//必须加行动算子或转换算子才会执行该算子内的代码
//        System.out.println(mapRDD.collect().toString());
        //2.5 打散分区
        JavaRDD<Tuple2<String, List<String>>> resultRDD = mapRDD.coalesce(500).repartition(200);





//        //Sink
//        JavaRDD<MapLinkRaw> repRDD = resultRDD.coalesce(500).repartition(200);
//        ss.createDataFrame(repRDD, MapLinkRaw.class).createOrReplaceTempView("sd_map_link_raw_tmp");
//        String outputSql = "insert overwrite table ad_d_cdm.dwd_sh_map_link_raw_hi partition (pt = '${cycle-1h}')\n" +
//                           "select vehicleId\n" +
//                           "\t , oldResponse\n" +
//                           "\t , newResponse\n" +
//                           "from sd_map_link_raw_tmp";
//        outputSql = StringUtil.templateStr(outputSql, map);
//
//        System.err.println(outputSql);
//        ss.sql(outputSql);

        //关闭SparkSession
        ss.stop();

    }



    /**
     * 数据装载
     * @param arrayList 轨迹集合
     * @return <oldResponse, newResponse>
     */
    private List<String> evaluateData(List<List<String>> arrayList) {

        if (arrayList == null) {
            return null;
        }
        //
//        ArrayList<List<String>> arrayLists = new ArrayList<>();
        //遍历list集合
//        for (int i = 0; i < arrList.size(); i++) {
//            String[] strArray = arrList.get(i).split(";");
//            List<String> list = Arrays.asList(strArray);
//            arrayLists.add(list);
//        }
        // URL
        String map_url = "http://apis.map.qq.com/ws/snaptoroads/v1/";
        // 设置请求体
        HashMap map = new HashMap();
        // 以k-v形式设置参数
        map.put("track", arrayList);
        map.put("key", "fcebca0fce22a2a47dfabc30c639f1f9");
        map.put("mode", "driving");
        map.put("smoothing", 1);
        map.put("snap_radius", 50);
        map.put("get_links", 2);
        String body = JSONObject.toJSONString(map);
        String oldRes = doPost(map_url, body);
        //压缩json为一行
//        oldRes = oldRes.replaceAll(StringUtils.LF, EMPTY)
//                .replaceAll("\\s{2,}", EMPTY)
//                .replaceAll("\\t", EMPTY)
//                .replaceAll(StringUtils.CR, EMPTY);

        //异常捕获
        oldRes = oldRes.replace("\r\n","").replace(" ","");
        String newRes = parses(oldRes);
        //links缺失:status=0/120/300/348/500...
        if(newRes.equals("[{}]")||newRes.equals("[]")){
            String status = ObjectUtil.obj2Str(JSONPath.eval(oldRes, "$.status"), "");
            String message = ObjectUtil.obj2Str(JSONPath.eval(oldRes, "$.message"), "");
            LOG.error("links缺失,响应状态:status=" + status + ", message=" + message);
            LOG.error("对应body:" + body);

            //当status=120,message=此key每秒请求量已达上限时 设置3次重试
            if(message.equals("此key每秒请求量已达上限")){
                for (int i = 0; i < 3; i++) {
                    try {
                        Thread.sleep(1000);
                        oldRes = doPost(map_url, body);
                        oldRes = oldRes.replace("\r\n","").replace(" ","");
                        newRes = parses(oldRes);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }
        }

        ArrayList<String> listRes = new ArrayList<>();
        listRes.add(oldRes);
        listRes.add(newRes);
        return listRes;
    }


    /**
     * post请求
     * @param httpurl
     * @param body
     * @return
     */
    public String doPost(String httpurl, String body) {
        HttpURLConnection connection = null;
        InputStream is = null;
        BufferedReader br = null;
        String result = null;// 返回结果字符串

        try {
            //人为固定qps=1,每1s调用一次
            Thread.sleep(1000);
            // 创建远程url连接对象
            URL url = new URL(httpurl);
            // 通过远程url连接对象打开一个连接，强转成httpURLConnection类
            connection = (HttpURLConnection) url.openConnection();
            // 请求方式为POST
            connection.setRequestMethod("POST");
            // 设置连接主机服务器的超时时间：15000毫秒
            connection.setConnectTimeout(15000);
            // 设置读取远程返回的数据时间：60000毫秒
            connection.setReadTimeout(60000);
            // 设置是否向conn输出(放在http正文内)
            connection.setDoOutput(Boolean.TRUE);
            connection.setDoInput(Boolean.TRUE);
            // 指定body为JSON格式
            connection.setUseCaches(false);
            connection.setRequestProperty("Content-type", "application/json");
            // 建立连接
            connection.connect();
            // 读取body
            StringBuffer sb = new StringBuffer();
            byte[] bytes = body.getBytes();
            OutputStream outputStream = connection.getOutputStream();
            outputStream.write(bytes);
            outputStream.flush();
            outputStream.close();
            // 通过connection连接，获取输入流
//            LOG.error("请求结果：" + connection.getResponseCode()); //200为正常请求
            if (connection.getResponseCode() == 200) {
                is = connection.getInputStream();
                // 封装输入流is，并指定字符集
                br = new BufferedReader(new InputStreamReader(is, "UTF-8"));
                // 存放数据
                StringBuffer sbf = new StringBuffer();
                String temp = null;
                while ((temp = br.readLine()) != null) {
                    sbf.append(temp);
                    sbf.append("\r\n");
                }
                // 获取返回结果
                result = sbf.toString();
            }
        } catch (MalformedURLException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } finally {
            // 关闭资源
            if (null != br) {
                try {
                    br.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            if (null != is) {
                try {
                    is.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            connection.disconnect();// 关闭远程连接
        }
        return result;
    }


    /**
     * 解析response
     * @param res
     * @return
     */
    public String parses(String res) {
        //处理pre_id或next_id
        NewResponse newResponse = new NewResponse();
        List<String> linkList = new ArrayList<>();
        String links = ObjectUtil.obj2Str(JSONPath.eval(res, "$.result.links"), "");

        try {
            String[] linksArr = links.replaceAll("(?:\\[\\[|\\]\\])", "").replace("},{", "};{").split(";");
            for (int i = 0; i < linksArr.length; i++) {
                newResponse.setIsReverse(ObjectUtil.obj2Int(JSONPath.eval(linksArr[i], "$.is_reverse"), null));
                newResponse.setTimestamp(ObjectUtil.obj2List(JSONPath.eval(linksArr[i], "$.timestamp")));
                newResponse.setOffset(ObjectUtil.obj2List(JSONPath.eval(linksArr[i], "$.offset")));
                newResponse.setReliability(ObjectUtil.obj2Int(JSONPath.eval(linksArr[i], "$.reliability"), null));
                newResponse.setLinkId(ObjectUtil.obj2Long(JSONPath.eval(linksArr[i], "$.id"), null));
                if (i + 1 < linksArr.length) { //避免空指针
                    newResponse.setNextLinkId(ObjectUtil.obj2Long(JSONPath.eval(linksArr[i + 1], "$.id"), null));
                } else {
                    newResponse.setNextLinkId(null);
                }
                linkList.add(JSONObject.toJSONString(newResponse));
            }
        }catch (Exception e){
//            LOG.error("抛出异常" + e);
        }
        return linkList.toString();
    }
}
