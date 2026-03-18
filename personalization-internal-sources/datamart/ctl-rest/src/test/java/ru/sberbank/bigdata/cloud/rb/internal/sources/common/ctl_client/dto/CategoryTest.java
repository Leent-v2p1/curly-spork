package ru.sberbank.bigdata.cloud.rb.internal.sources.common.ctl_client.dto;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

class CategoryTest {

    @Test
    void equalsTst() {
        Category category = new Category(1, "one", false, 2, 3, "no");
        Category categorySame = new Category(1, "one", false, 2, 3, "no");
        Category categoryDiff = new Category(2, "two", true, 4, 5, "yes");

        assertNotEquals(category, categoryDiff);
        assertEquals(category, categorySame);
    }

    @Test
    void hashCodeTest() {
        Category category = new Category(1, "one", false, 2, 3, "no");
        assertEquals(-370834447, category.hashCode());
    }
}