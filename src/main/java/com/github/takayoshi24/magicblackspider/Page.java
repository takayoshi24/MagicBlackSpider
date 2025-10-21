package com.github.takayoshi24.magicblackspider;

import org.jsoup.nodes.Document;


public class Page {
    private final String url;
    private final Document document;


    public Page(String url, Document document) {
        this.url = url;
        this.document = document;
    }


    public String getUrl() { return url; }
    public Document getDocument() { return document; }
}