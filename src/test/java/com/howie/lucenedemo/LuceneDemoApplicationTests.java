package com.howie.lucenedemo;

import com.howie.lucenedemo.Service.DemoService;
import org.apache.lucene.document.*;

import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;


import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@SpringBootTest
class LuceneDemoApplicationTests {


    @Autowired
    DemoService demoService;

    /**
     * 1.1 增量更新索引-单条
     */
    @Test
    void updateTest01() throws Exception {
        // 创建要更新的文档
        Document doc = new Document();
        doc.add(new StringField("id", "1", Field.Store.YES));
        doc.add(new TextField("title", "updateTest01", Field.Store.YES));
        ArrayList<Document> documents = new ArrayList<>();
        documents.add(doc);
        //增量更新
        demoService.updateIndex(documents);
        //查询更新内容（第1页，每页10条）
        List<Document> documentList = demoService.searchDocuments("updateTest01", "title", 1, 10);
        //结果判断
        assertEquals(1, documentList.size());
    }

    /**
     * 1.2 增量更新索引-多条
     */
    @Test
    void updateTest02() throws Exception {
        ArrayList<Document> documents = new ArrayList<>();
        // 创建要更新的文档
        for (int i = 0; i < 100; i++) {
            Document doc = new Document();
            doc.add(new StringField("id", String.valueOf(i), Field.Store.YES));
            doc.add(new TextField("title", "updateTest02" + i, Field.Store.YES));
            documents.add(doc);
        }
        demoService.updateIndex(documents);
        //查询更新内容（第10页，每页10条）
        List<Document> documentList = demoService.searchDocuments("updateTest02*", "title", 10, 10);
        //结果判断
        assertEquals(10, documentList.size());
    }

    /**
     * 2.1 分页搜索
     */
    @Test
    void searchTest01() throws Exception {
        //获取第2页数据(每页100条)
        List<Document> documents = demoService.searchDocuments("Document*", "title", 2, 100);
        //结果判断
        assertEquals(100, documents.size());
    }

    /**
     * 2.1 分页搜索-按Id排序
     *//*
    @Test
    void searchTest02() throws Exception {

        SortField sortField = new SortField("id", SortField.Type.STRING);
        Sort sort = new Sort(sortField);

        //获取第2页数据(每页100条)
        List<Document> documents = demoService.searchDocuments("Document*", "title", 2, 100, sort);
        //结果判断
        assertEquals(100, documents.size());
    }

    *//**
     * 2.1 分页搜索-多值匹配
     *//*
    @Test
    void searchTest03() throws Exception {

        String[] values = {"Document100", "Document101", "Document202"};

        SortField sortField = new SortField("title", SortField.Type.STRING);
        Sort sort = new Sort(sortField);

        //
        List<Document> documents = demoService.searchDocuments(values, "title", 1, 10);
        //结果判断
        assertEquals(3, documents.size());
    }*/



    /**
     * 3.1 查询满足条件的文档数
     */
    @Test
    void countTest() throws Exception {
        long countedDocuments = demoService.countDocuments("title", "Document*");
        System.out.println("countedDocuments：" + countedDocuments);
        //结果判断
        assertTrue(countedDocuments > 0);
    }

}
