package com.github.takayoshi24.magicblackspider.handler;

import com.github.takayoshi24.magicblackspider.Page;
import com.github.takayoshi24.magicblackspider.Scheduler;


public interface PageHandler {
    void handle(Page page, Scheduler scheduler);
}