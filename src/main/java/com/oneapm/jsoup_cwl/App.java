package com.oneapm.jsoup_cwl;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Consumer;
import java.util.logging.ConsoleHandler;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import lombok.Getter;
import lombok.Setter;

import org.apache.commons.lang3.StringUtils;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.unit.TimeValue;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.nodes.Node;
import org.jsoup.nodes.TextNode;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Hello world!
 */
public class App {
    
    public static Logger                             log             = Logger.getAnonymousLogger();
    
    private static final String                      SPLIT_DELIMITER = "\\s+";
    
    private static ObjectMapper                      json            = new ObjectMapper();
    
    private static final LinkedBlockingQueue<String> queue           = new LinkedBlockingQueue<String>();
    
    private static LongAdder                         statics         = new LongAdder();
    
    private static Settings                          settings        = null;
    
    private static TransportClient                   client          = null;
    
    // TODO
    private static ExecutorService                   pool            = Executors.newCachedThreadPool();
    
    // TODO
    private static ScheduledExecutorService          scheduler       = Executors.newSingleThreadScheduledExecutor();
    
    static {
        // init log
        Handler handler = new ConsoleHandler();
        handler.setLevel(Level.ALL);
        log.setLevel(Level.ALL);
        log.addHandler(handler);
        log.info("Statics:Active:TaskCount");
        //
        scheduler.scheduleAtFixedRate(() -> {
                                          try {
                                              log.info(statics.sum() +
                                                       "\t\t" +
                                                       ((ThreadPoolExecutor) pool).getActiveCount() +
                                                       "\t\t" +
                                                       ((ThreadPoolExecutor) pool).getTaskCount());
                                          } catch (Exception e) {
                                              log.severe("Monitor fall in exception: " +
                                                         e);
                                          }
                                      },
                                      0L,
                                      1L,
                                      TimeUnit.SECONDS);
    }
    
    /**
     * ClassName: Box <br/>
     * Function: <br/>
     * date: 2016年8月20日 下午7:42:11 <br/>
     *
     * @author shengjie
     * @version App
     * @since JDK 1.8
     */
    @Getter
    @Setter
    private static class Box {
        
        private String  date;
        
        private Integer r1;
        
        private Integer r2;
        
        private Integer r3;
        
        private Integer r4;
        
        private Integer r5;
        
        private Integer r6;
        
        private Integer b1;
        
        /**
         * toJSON: <br/>
         * 
         * @author shengjie
         * @return
         * @since JDK 1.8
         */
        public String toJSON() {
            try {
                return json.writeValueAsString(this);
            } catch (JsonProcessingException e) {
                log.severe("Convert to JSON string failed: " +
                           e);
                return "";
            }
        }
        
    }
    
    /**
     * ClassName: Processor <br/>
     * Function: <br/>
     * date: 2016年8月20日 下午3:04:29 <br/>
     *
     * @author shengjie
     * @version App
     * @since JDK 1.8
     */
    private static class Processor {
        
        /**
         * toConsole: <br/>
         * 
         * @author shengjie
         * @param box
         * @since JDK 1.8
         */
        public static void toConsole(Box box) {
            System.out.println(box.toJSON());
        }
        
        /**
         * toLog: <br/>
         * 
         * @author shengjie
         * @param box
         * @since JDK 1.8
         */
        public static void toLog(Box box) {
            log.info(box.toJSON());
        }
        
        /**
         * toES: <br/>
         * 
         * @author shengjie
         * @param box
         * @since JDK 1.8
         */
        public static void toES(Box box) {
            //
            if (settings == null) {
                settings = Settings.settingsBuilder()
                                   .put("cluster.name",
                                        "elasticsearch")
                                   .build();
            }
            //
            if (client == null) {
                try {
                    client = TransportClient.builder()
                                            .settings(settings)
                                            .build()
                                            .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("127.0.0.1"),
                                                                                                9300));
                } catch (UnknownHostException e) {
                    log.severe("Connect to ES server failed: " +
                               e);
                    System.exit(-1);
                }
            }
            //
            IndexResponse response = client.prepareIndex("test_1",
                                                         "logs")
                                           .get(TimeValue.timeValueSeconds(3));
            log.finest(response.toString());
        }
        
        /**
         * toAll: <br/>
         * 
         * @author shengjie
         * @param box
         * @since JDK 1.8
         */
        public static void toAll(Box box) {
            toConsole(box);
            toLog(box);
            toES(box);
        }
        
    }
    
    /**
     * ClassName: Parser <br/>
     * Function: <br/>
     * date: 2016年8月20日 下午3:04:31 <br/>
     *
     * @author shengjie
     * @version App
     * @since JDK 1.8
     */
    private static class Parser {
        
        /**
         * parseTable: <br/>
         * 
         * @author shengjie
         * @param doc
         * @return
         * @since JDK 1.8
         */
        public static Optional<Element> parseTable(Document doc) {
            return Optional.ofNullable(doc.select("table.hz")
                                          .first());
        }
        
        /**
         * parseBody: <br/>
         * 
         * @author shengjie
         * @param elem
         * @return
         * @since JDK 1.8
         */
        public static Optional<Element> parseBody(Element elem) {
            return Optional.ofNullable(elem.select("tbody")
                                           .first());
        }
        
        /**
         * parseNode: <br/>
         * 
         * @author shengjie
         * @param elem
         * @return
         * @since JDK 1.8
         */
        public static Stream<Node> parseNode(Element elem) {
            return elem.childNodes()
                       .stream()
                       .filter(node -> node instanceof Element)
                       .skip(2);
        }
        
        /**
         * parseBox: <br/>
         * 
         * @author shengjie
         * @param node
         * @return
         * @since JDK 1.8
         */
        public static Optional<Box> parseBox(Node node) {
            try {
                Box box = new Box();
                //
                Element[] n1_elem = node.childNodes()
                                        .stream()
                                        .filter(child -> child instanceof Element)
                                        .toArray(Element[]::new);
                //
                box.setDate(((TextNode) (n1_elem[0].childNodes()
                                                   .stream()
                                                   .findFirst().get())).text()
                                                                       .trim()
                                                                       .split(SPLIT_DELIMITER)[0].trim());
                //
                Integer[] reds = n1_elem[1].childNodes()
                                           .stream()
                                           .filter(child -> child instanceof Element)
                                           .findFirst()
                                           .get()
                                           .childNodes()
                                           .stream()
                                           .filter(child -> child instanceof Element)
                                           .map(red -> ((TextNode) (red.childNodes()
                                                                       .stream()
                                                                       .filter(child -> child instanceof TextNode)
                                                                       .findFirst().get())).text()
                                                                                           .trim()
                                                                                           .split(SPLIT_DELIMITER)[0].trim())
                                           .map(r -> Integer.valueOf(r.trim()))
                                           .toArray(Integer[]::new);
                box.setR1(reds[0]);
                box.setR2(reds[1]);
                box.setR3(reds[2]);
                box.setR4(reds[3]);
                box.setR5(reds[4]);
                box.setR6(reds[5]);
                //
                box.setB1(Integer.valueOf(((TextNode) (n1_elem[2].childNodes()
                                                                 .stream()
                                                                 .filter(child -> child instanceof Element)
                                                                 .findFirst()
                                                                 .get()
                                                                 .childNodes()
                                                                 .stream()
                                                                 .filter(child -> child instanceof Element)
                                                                 .findFirst()
                                                                 .get()
                                                                 .childNodes()
                                                                 .stream()
                                                                 .filter(child -> child instanceof TextNode)
                                                                 .findFirst().get())).text()
                                                                                     .trim()
                                                                                     .split(SPLIT_DELIMITER)[0].trim()));
                //
                statics.increment();
                return Optional.ofNullable(box);
            } catch (Exception e) {
                log.severe("Parse Node to Box failed: " +
                           e);
                return Optional.empty();
            }
        }
    }
    
    /**
     * ClassName: Cwl <br/>
     * Function: <br/>
     * date: 2016年8月20日 下午3:04:34 <br/>
     *
     * @author shengjie
     * @version App
     * @since JDK 1.8
     */
    private static class Cwl {
        
        /**
         * getDocument: <br/>
         * 
         * @author shengjie
         * @param url
         * @return
         * @since JDK 1.8
         */
        public static Optional<Document> getDocument(Optional<String> url) {
            try {
                return Optional.ofNullable(Jsoup.connect(url.isPresent()
                                                                        ? url.get()
                                                                        : "")
                                                .get());
            } catch (IOException e) {
                log.severe("Get document from url " +
                           url +
                           " failed: " +
                           e);
                // add
                url.ifPresent(u -> {
                    if (!StringUtils.isBlank(u))
                        queue.offer(u);
                });
                return Optional.empty();
            }
        }
        
    }
    
    /**
     * spider: <br/>
     * 
     * @author shengjie
     * @param consumer
     * @since JDK 1.8
     */
    private static void spider(Consumer<? super Box> consumer) {
        while (!queue.isEmpty()) {
            log.info("开始对[" +
                     queue.size() +
                     "]规模的URL流进行网页抓取处理");
            //
            String[] urls = queue.toArray(new String[0]);
            // here queue is be cleared, but urls would not be affected
            queue.clear();
            //
            Stream.of(urls)
                  .parallel()
                  .map(Optional::ofNullable)
                  .map(Cwl::getDocument)
                  .filter(o_doc -> o_doc.isPresent())
                  .map(o_doc -> o_doc.get())
                  .map(Parser::parseTable)
                  .filter(o_table -> o_table.isPresent())
                  .map(o_table -> o_table.get())
                  .map(Parser::parseBody)
                  .filter(o_body -> o_body.isPresent())
                  .map(o_body -> o_body.get())
                  .flatMap(Parser::parseNode)
                  .map(Parser::parseBox)
                  .filter(o_box -> o_box.isPresent())
                  .map(o_box -> o_box.get())
                  .forEach(consumer);
        }
    }
    
    /**
     * main: <br/>
     * 
     * @author shengjie
     * @param args
     * @throws IOException
     * @since JDK 1.8
     */
    public static void main(String[] args) throws IOException {
        //
        assert args.length == 4;
        //
        IntStream.range(Integer.valueOf(args[0]),
                        Integer.valueOf(args[1]))
                 .parallel()
                 .mapToObj(i -> args[2] +
                                ((i - 1) > 0
                                            ? "_" +
                                              (i - 1)
                                            : "") +
                                args[3])
                 .forEach(url -> queue.offer(url));
        //
        spider(Processor::toLog);
    }
    
}
