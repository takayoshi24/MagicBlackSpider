package com.github.takayoshi24.magicblackspider.utils;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class RobotsTxtCheckerTest {

    private final RobotsTxtChecker checker = new RobotsTxtChecker("TestBot");

    // --- Fix 1: port included in base URL ---

    @Test
    void isAllowed_standardPort_noPortInBaseUrl() throws Exception {
        // getAuthority() on http://example.com/page returns "example.com" (no port suffix for standard ports)
        java.net.URL u = new java.net.URL("http://example.com/page");
        assertEquals("example.com", u.getAuthority());
    }

    @Test
    void isAllowed_nonStandardPort_portIncludedInBaseUrl() throws Exception {
        // getAuthority() on http://example.com:8080/page returns "example.com:8080"
        java.net.URL u = new java.net.URL("http://example.com:8080/page");
        assertEquals("example.com:8080", u.getAuthority());
    }

    // --- Fix 2: specificity-based rule matching ---

    @Test
    void isAllowed_allowMoreSpecificThanDisallow_returnsAllowed() {
        RobotsTxtChecker.RobotsTxtRules rules = new RobotsTxtChecker.RobotsTxtRules();
        rules.disallows.add("/foo");
        rules.allows.add("/foobar");

        // /foobar/page — Allow: /foobar (7 chars) beats Disallow: /foo (4 chars)
        assertTrue(checker.isAllowed("http://example.com/foobar/page", rules, "http://example.com"));
    }

    @Test
    void isAllowed_disallowMoreSpecificThanAllow_returnsDisallowed() {
        RobotsTxtChecker.RobotsTxtRules rules = new RobotsTxtChecker.RobotsTxtRules();
        rules.allows.add("/foo");
        rules.disallows.add("/foobar");

        // /foobar/page — Disallow: /foobar (7 chars) beats Allow: /foo (4 chars)
        assertFalse(checker.isAllowed("http://example.com/foobar/page", rules, "http://example.com"));
    }

    @Test
    void isAllowed_equalSpecificity_allowWins() {
        RobotsTxtChecker.RobotsTxtRules rules = new RobotsTxtChecker.RobotsTxtRules();
        rules.allows.add("/foo");
        rules.disallows.add("/foo");

        // Tie (same length) → Allow wins per spec
        assertTrue(checker.isAllowed("http://example.com/foo/bar", rules, "http://example.com"));
    }

    @Test
    void isAllowed_onlyDisallow_returnsDisallowed() {
        RobotsTxtChecker.RobotsTxtRules rules = new RobotsTxtChecker.RobotsTxtRules();
        rules.disallows.add("/private");

        assertFalse(checker.isAllowed("http://example.com/private/data", rules, "http://example.com"));
    }

    @Test
    void isAllowed_onlyAllow_returnsAllowed() {
        RobotsTxtChecker.RobotsTxtRules rules = new RobotsTxtChecker.RobotsTxtRules();
        rules.allows.add("/public");

        assertTrue(checker.isAllowed("http://example.com/public/page", rules, "http://example.com"));
    }

    @Test
    void isAllowed_noRules_returnsAllowed() {
        RobotsTxtChecker.RobotsTxtRules rules = new RobotsTxtChecker.RobotsTxtRules();

        assertTrue(checker.isAllowed("http://example.com/anything", rules, "http://example.com"));
    }

    @Test
    void isAllowed_noMatchingRule_returnsAllowed() {
        RobotsTxtChecker.RobotsTxtRules rules = new RobotsTxtChecker.RobotsTxtRules();
        rules.disallows.add("/admin");

        assertTrue(checker.isAllowed("http://example.com/public/page", rules, "http://example.com"));
    }
}
