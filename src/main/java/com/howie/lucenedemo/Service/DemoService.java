package com.howie.lucenedemo.Service;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.*;
import org.apache.lucene.index.*;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.*;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.MMapDirectory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

@Service
public class DemoService {

    //日志对象
    Logger log = LoggerFactory.getLogger(DemoService.class);

    //分析器
    private final StandardAnalyzer analyzer;

    private final IndexWriter writer;

    private IndexReader reader;

    private IndexSearcher searcher;

    //读写锁
    private final ReadWriteLock lock = new ReentrantReadWriteLock();

    //构造参数初始化
    public DemoService(@Value("${lucene.index.path}") String indexPath) throws IOException {
        log.info("IndexPath: " + indexPath);
        //配置文件映射
        Directory directory = new MMapDirectory(Paths.get(indexPath));
        //初始化分词器analyzer
        this.analyzer = new StandardAnalyzer();
        //创建配置和writer写入对象
        IndexWriterConfig config = new IndexWriterConfig(analyzer);
        this.writer = new IndexWriter(directory, config);
        //初始化数据
        createIndex();
        //初始化reader
        this.reader = DirectoryReader.open(writer);
        //初始化searcher
        this.searcher = new IndexSearcher(this.reader);

    }

    // 创建示例初始文档数据
    private List<Document> createInitialDocuments() {
        //documents集合构造
        List<Document> documents = new ArrayList<>();
        for (long i = 1; i <= 1000; i++) {
            Document doc = new Document();
            doc.add(new StringField("id", String.valueOf(i), Field.Store.YES));
            doc.add(new TextField("title", "Document" + i, Field.Store.YES));
            doc.add(new TextField("status", "status" + i, Field.Store.YES));
            doc.add(new StringField("time", String.valueOf(System.currentTimeMillis()), Field.Store.YES));
            documents.add(doc);
        }
        return documents;
    }

    // 初始化索引
    private void createIndex() throws IOException {
        // 示例初始文档数据
        List<Document> initialDocuments = createInitialDocuments();
        log.info("InitialDocumentsSize: " + initialDocuments.size());
        for (Document doc : initialDocuments) {
            writer.addDocument(doc);
        }
    }

    /**
     * 增量更新索引
     * @param documents 更新的文档
     * @throws IOException 更新过程中的IO异常
     */
    public void updateIndex(List<Document> documents) throws IOException {
        lock.writeLock().lock();
        try {
            for (Document doc : documents) {
                writer.updateDocument(new Term("id", doc.get("id")), doc);
            }
            //数据更新后刷新reader和searcher
            this.reader = DirectoryReader.open(writer);
            this.searcher = new IndexSearcher(reader);
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * 分页查询
     * @param queryStr 匹配内容
     * @param field 匹配字段
     * @param pageNumber 页码
     * @param pageSize 每页数量
     * @return 文档结果集
     * @throws Exception 查询过程中的异常信息
     */
    public List<Document> searchDocuments(String queryStr, String field, int pageNumber,
                                          int pageSize) throws Exception {
        lock.readLock().lock();
        try {
            // 创建搜索对象
            Query query = new QueryParser(field, analyzer).parse(queryStr);
            // 执行查询
            TopDocs topDocs = searcher.search(query, pageNumber * pageSize);
            // 计算起始和结束文档索引
            return getDocuments(pageNumber, pageSize, topDocs);
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * 分页查询（排序）
     * @param queryStr 匹配内容
     * @param field 匹配字段
     * @param pageNumber 页码
     * @param pageSize 每页数量
     * @return 文档结果集
     * @throws Exception 查询过程中的异常信息
     */
    public List<Document> searchDocuments(String queryStr, String field, int pageNumber,
                                          int pageSize, Sort sort) throws Exception {
        lock.readLock().lock();
        try {
            // 创建搜索对象
            Query query = new QueryParser(field, analyzer).parse(queryStr);
            // 执行查询
            TopDocs topDocs = searcher.search(query, pageNumber * pageSize, sort);
            // 计算起始和结束文档索引
            return getDocuments(pageNumber, pageSize, topDocs);
        } finally {
            lock.readLock().unlock();
        }
    }

    private List<Document> getDocuments(int pageNumber, int pageSize, TopDocs topDocs) throws IOException {
        int start = (pageNumber - 1) * pageSize;
        long end = Math.min(topDocs.totalHits.value, (long) pageNumber * pageSize);
        // 组装结果集
        List<Document> results = new ArrayList<>();
        for (int i = start; i < end; i++) {
            int docId = topDocs.scoreDocs[i].doc;
            Document document = searcher.doc(docId);
            results.add(document);
        }
        return results;
    }

    /**
     * 时间范围查询
     * @param query 时间范围条件
     * @param pageNumber 页码
     * @param pageSize 每页数量
     * @return 文档结果集
     * @throws Exception 查询过程中的异常信息
     */
    public List<Document> searchDocuments(Query query, int pageNumber, int pageSize) throws Exception {
        lock.readLock().lock();
        try {
            // 执行查询
            TopDocs topDocs = searcher.search(query, pageNumber * pageSize);
            // 计算起始和结束文档索引
            return getDocuments(pageNumber, pageSize, topDocs);
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * 多值匹配查询
     * @param values 匹配内容
     * @param field 匹配字段
     * @param pageNumber 页码
     * @param pageSize 每页数量
     * @return 文档结果集
     * @throws Exception 查询过程中的异常信息
     */
    public List<Document> searchDocuments(String[] values, String field, int pageNumber,
                                          int pageSize, Sort sort) throws Exception {
        lock.readLock().lock();
        try {
            // 创建多值匹配对象
            BooleanQuery.Builder booleanQueryBuilder = new BooleanQuery.Builder();
            for (String value : values) {
                TermQuery termQuery = new TermQuery(new Term(field, value));
                booleanQueryBuilder.add(termQuery, BooleanClause.Occur.SHOULD);
            }
            Query multiValueQuery = booleanQueryBuilder.build();
            // 执行查询
            TopDocs topDocs = searcher.search(multiValueQuery, pageNumber * pageSize, sort);
            // 计算起始和结束文档索引
            return getDocuments(pageNumber, pageSize, topDocs);
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * 查询满足条件的文档数
     * @param field 查询字段
     * @param queryStr 匹配内容
     * @return 符合条件的数量
     * @throws Exception 查询过程中的异常信息
     */
    public long countDocuments(String field, String queryStr) throws Exception {
        lock.readLock().lock();
        try {
            Query query = new QueryParser(field, analyzer).parse(queryStr);
            // 执行查询
            TopDocs topDocs = searcher.search(query, Integer.MAX_VALUE);
            return topDocs.totalHits.value;
        } finally {
            lock.readLock().unlock();
        }
    }

}
