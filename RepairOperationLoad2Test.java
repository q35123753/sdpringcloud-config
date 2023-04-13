package com.digitalchina.livable;

import cn.hutool.core.io.FileUtil;
import cn.hutool.core.io.file.FileReader;
import cn.hutool.core.util.StrUtil;
import com.digitalchina.base.utils.BeanUtil;
import com.digitalchina.livable.modules.repair.mapper.RepairOperationMapper;
import com.digitalchina.livable.modules.repair.vo.SelectAllFromAuditVo;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.annotation.Rollback;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.io.File;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 多线程下载维修审计图片
 * https://blog.csdn.net/LJ_759482/article/details/124588615
 *
 * @author youyz
 * @name RepairOperationLoad2Test
 * @date 2023-04-03 9:55
 */

@RunWith(SpringJUnit4ClassRunner.class)
@Rollback(value = false)
@SpringBootTest(classes = HousingRentalAdminApplication.class)
public class RepairOperationLoad2Test {
    private static Logger logger = LoggerFactory.getLogger(RepairOperationLoad2Test.class);

    private static final String DOWNLOAD_DIR = "C:\\data\\repairImg";

    private static final String REP_NO_DIR = "C:\\data\\维修单号.txt";


    @Autowired
    RepairOperationMapper repairOperationMapper;

    @Test
    public void downloadImg() {

        // 维修单list
        List<String> repairNoList = getRepairNoListByFile(REP_NO_DIR);

        // 流程id list
        //List<Long> procdefId = new ArrayList<>();

        // 下载地址map
        Map<String, String> loadMap = new HashMap<>();

        // 查询出对应的下载地址
        List<SelectAllFromAuditVo> operationList = repairOperationMapper.selectAllFromAudit(repairNoList, null);

        for (SelectAllFromAuditVo selectAllFromAuditVo : operationList) {
            String pictureUrl = selectAllFromAuditVo.getPictureUrl();
            String pictureName = selectAllFromAuditVo.getPictureName();
            List<String> pictureUrlList = StrUtil.split(pictureUrl, ',');
            List<String> pictureNameList = StrUtil.split(pictureName, ',');



            for (int i = 0; i < pictureUrlList.size(); i++) {
                SelectAllFromAuditVo loadImgs = new SelectAllFromAuditVo();
                BeanUtil.copy(selectAllFromAuditVo, loadImgs);
                loadImgs.setPictureUrl(pictureUrlList.get(i));
                loadImgs.setPictureName(pictureNameList.get(i));

                //文件路径不存在 则创建
                String procdefText = loadImgs.getProcdefText();
                procdefText = procdefText == null ? loadImgs.getOldProcdefText() : procdefText;
                String dirPath = DOWNLOAD_DIR + "\\" + loadImgs.getRepairNo().trim() + "\\" + procdefText;
                File sf = new File(dirPath);
                if (!sf.exists()) {
                    sf.mkdirs();
                }
                // 具体的路径  加上文件名
                dirPath = DOWNLOAD_DIR + "\\" + loadImgs.getRepairNo().trim() + "\\" + procdefText + "\\" + loadImgs.getPictureName();
                // 组装map  后续放在线程池下载
                loadMap.put(loadImgs.getPictureUrl(), dirPath);
            }
        }
        logger.info("下载开始,文件数量为: {}", loadMap.size());
        DownloadUtil.batchDownLoad(loadMap);
    }

    // 从文件里获取出维修单号
    public static List<String> getRepairNoListByFile(String filePath) {
        FileReader reader = FileReader.create(FileUtil.file(filePath)); // 创建文件读取器
        String content = reader.readString(); // 读取整个文件内容
        String[] lines = content.split(System.lineSeparator()); // 按行分割字符串
        return Arrays.asList(lines);
    }

}


class ThreadUtil {

    /**
     * 创建下载线程池
     *
     * @param threadSize 线程数量
     * @return ExecutorService
     */
    public static ExecutorService buildDownThreadPool(int threadSize) {
        // 空闲线程存活时间
        long keepAlive = 0L;
        // 下载线程名称前缀
        String dwonThreadNamePrefix = "dwonThread-pool";
        // 构建线程工厂
        ThreadFactory factory = ThreadUtil.buildThreadFactory(dwonThreadNamePrefix);
        //创建线程池
        return new ThreadPoolExecutor(threadSize,
                threadSize,
                keepAlive,
                TimeUnit.SECONDS,
                new ArrayBlockingQueue<>(threadSize),
                factory);
    }

    /**
     * 创建自定义线程工厂
     *
     * @param threadNamePrefix 线程名称前缀
     * @return ThreadFactory
     */
    public static ThreadFactory buildThreadFactory(String threadNamePrefix) {
        return new CustomThreadFactory(threadNamePrefix);
    }

    /**
     * 自定义线程工厂
     */
    public static class CustomThreadFactory implements ThreadFactory {
        //线程名称前缀
        private String threadNamePrefix;

        private AtomicInteger counter = new AtomicInteger(1);

        /**
         * 自定义线程工厂
         *
         * @param threadNamePrefix 线程名称前缀
         */
        CustomThreadFactory(String threadNamePrefix) {
            this.threadNamePrefix = threadNamePrefix;
        }

        @Override
        public Thread newThread(Runnable r) {
            String threadName = threadNamePrefix + "-t" + counter.getAndIncrement();
            return new Thread(r, threadName);
        }
    }
}


class DownloadUtil {

    private static Logger logger = LoggerFactory.getLogger(DownloadUtil.class);

    /**
     * 下载线程数
     */
    private static final int DOWNLOAD_THREAD_NUM = 8;

    /**
     * 下载线程池
     */
    private static ExecutorService downloadExecutorService = ThreadUtil.buildDownThreadPool(DOWNLOAD_THREAD_NUM);

    /**
     * 单文件下载
     *
     * @param fileUrl 文件url,如:https://img3.doubanio.com//view//photo//s_ratio_poster//public//p2369390663.webp
     * @param path    存放路径,如:D:/opt/img/douban/my.webp
     */
    public static void download(String fileUrl, String path) throws Exception {
        // 判断存储文件夹是否已经存在或者创建成功
        if (!createFolderIfNotExists(path)) {
            return;
        }
        HttpURLConnection conn = null;
        InputStream in = null;
        FileOutputStream out = null;
        try {
            URL url = new URL(fileUrl);
            conn = (HttpURLConnection) url.openConnection();
            conn.setRequestMethod("GET");
            conn.setConnectTimeout(10000);
            in = conn.getInputStream();
            out = new FileOutputStream(path);
            int len;
            byte[] arr = new byte[1024 * 1000];
            while (-1 != (len = in.read(arr))) {
                out.write(arr, 0, len);
            }
            out.flush();
        } catch (Exception e) {
            logger.error("Fail to download: {} by {}", fileUrl, e.getMessage());
            throw new Exception(e.getMessage());
        } finally {
            try {
                if (null != conn) {
                    conn.disconnect();
                }
                if (null != out) {
                    out.close();
                }
                if (null != in) {
                    in.close();
                }
            } catch (Exception e) {
                logger.error("Error to close stream: {}", e.getMessage());
                throw new Exception(e.getMessage());
            }
        }
    }

    /**
     * 批量多线程下载资源
     *
     * @param resourceMap 资源map, key为资源下载url,value为资源存储位置
     */
    public static void batchDownLoad(Map<String, String> resourceMap) {
        if (Objects.isNull(resourceMap) || resourceMap.isEmpty()) {
            return;
        }
        try {
            List<String> keys = new ArrayList<>(resourceMap.keySet());
            int size = keys.size();
            int pageNum = getPageNum(size);
            long startTime = System.currentTimeMillis();
            for (int index = 0; index < pageNum; index++) {
                int start = index * DOWNLOAD_THREAD_NUM;
                int last = getLastNum(size, start + DOWNLOAD_THREAD_NUM);
                //线程计数器
                final CountDownLatch latch = new CountDownLatch(last - start);
                // 获取列表子集
                List<String> urlList = keys.subList(start, last);
                for (String url : urlList) {
                    // 提交任务
                    Runnable task = new DownloadWorker(latch, url, resourceMap.get(url));
                    downloadExecutorService.submit(task);
                }
                latch.await();
            }
            long endTime = System.currentTimeMillis();
            logger.info("Download total time " + (endTime - startTime) + " millisSeconds");
        } catch (Exception e) {
            logger.error("{}", e);
        } finally {
            //关闭线程池
            if (null != downloadExecutorService) {
                downloadExecutorService.shutdown();
            }
        }
    }

    /**
     * 创建文件夹,如果文件夹已经存在或者创建成功返回true
     *
     * @param path 路径
     * @return boolean
     */
    private static boolean createFolderIfNotExists(String path) {
        String folderName = getFolder(path);
        if (folderName.equals(path)) {
            return true;
        }
        File folder = new File(getFolder(path));
        if (!folder.exists()) {
            synchronized (DownloadUtil.class) {
                if (!folder.exists()) {
                    return folder.mkdirs();
                }
            }
        }
        return true;
    }

    /**
     * 获取文件夹
     *
     * @param path 文件路径
     * @return String
     */
    private static String getFolder(String path) {
        int index = path.lastIndexOf("/");
        return -1 != index ? path.substring(0, index) : path;
    }

    /**
     * 获取最后一个元素
     *
     * @param size  列表长度
     * @param index 下标
     * @return int
     */
    private static int getLastNum(int size, int index) {
        return index > size ? size : index;
    }

    /**
     * 获取划分页面数量
     *
     * @param size 列表长度
     * @return int 划分页面数量
     */
    private static int getPageNum(int size) {
        int tmp = size / DOWNLOAD_THREAD_NUM;
        return size % DOWNLOAD_THREAD_NUM == 0 ? tmp : tmp + 1;
    }

    /**
     * 下载线程
     */
    static class DownloadWorker implements Runnable {

        private CountDownLatch latch;
        private String url;
        private String path;

        DownloadWorker(CountDownLatch latch, String url, String path) {
            this.latch = latch;
            this.url = url;
            this.path = path;
        }

        @Override
        public void run() {
            try {
                logger.debug("Start batch:[{}] into: [{}]", url, path);
                DownloadUtil.download(url, path);
                logger.debug("Download:[{}] into: [{}] is done", url, path);
            } catch (Exception e) {
            } finally {
                latch.countDown();
            }
        }
    }

    public static void main(String[] args) {
        Map<String, String> map = new HashMap<>();
        map.put("https://dcw-guangzhou1.oss-cn-shenzhen.aliyuncs.com/bd40b8f5c4f243029c29e7718e95ae56.jpeg?Expires=17446437535&OSSAccessKeyId=LTAI4GKY8JsdUdhXd63zcrpY&Signature=M3bxagWP1psix7jXhTK0PMvlC20%3D", "D:/test/11/1.png");
        map.put("https://dcw-guangzhou1.oss-cn-shenzhen.aliyuncs.com/d97ae86b91fc46d8bf1aca3400816903.jpeg?Expires=17446437535&OSSAccessKeyId=LTAI4GKY8JsdUdhXd63zcrpY&Signature=XYGfa6FMdoBo3n9%2Ftaql4bN%2BRfg%3D", "D:/test/11/2.png");
        map.put("https://dcw-guangzhou1.oss-cn-shenzhen.aliyuncs.com/17c3c03c27154ad090b0da71cc7064e3.jpeg?Expires=17446437535&OSSAccessKeyId=LTAI4GKY8JsdUdhXd63zcrpY&Signature=Y4wGkILHuCr1dN%2FjoX8Vr5HHDS4%3D", "D:/test/11/3.png");
        DownloadUtil.batchDownLoad(map);
    }
}
