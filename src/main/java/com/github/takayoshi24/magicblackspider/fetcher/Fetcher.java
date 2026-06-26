package com.github.takayoshi24.magicblackspider.fetcher;

import org.jsoup.nodes.Document;

import java.io.IOException;


public interface Fetcher {
    Document fetch(String url) throws IOException;
}