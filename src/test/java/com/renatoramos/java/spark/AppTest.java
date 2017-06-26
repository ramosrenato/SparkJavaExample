package com.renatoramos.java.spark;

import org.junit.Test;
import static org.junit.Assert.assertEquals;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

public class AppTest {

    public AppTest() {
        Logger.getLogger("org").setLevel(Level.ERROR);
    }

    @Test
    public void testRunMethod() {

        String fileInput = "src\\test\\resources\\words.txt";
        String word = "mais";
        int expectedValue = 14;

        WordCount wc = new WordCount();
        wc.run(fileInput);

        assertEquals(expectedValue, wc.getWords().get(word).intValue());
    }

}