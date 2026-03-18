package ru.sberbank.bigdata.cloud.rb.internal.sources.generator.impl.replica_on_date;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.constants.NameAdditions;
import ru.sberbank.bigdata.cloud.rb.internal.sources.generator.api.oozie.generators.parser.beans.Action;
import ru.sberbank.bigdata.cloud.rb.internal.sources.generator.api.oozie.generators.parser.beans.ActionImpl;
import ru.sberbank.bigdata.cloud.rb.internal.sources.generator.api.oozie.generators.parser.beans.Fork;

import java.util.Arrays;
import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.collection.IsCollectionWithSize.hasSize;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ForkCreatorTest {

    private final ForkCreator forkCreator = new ForkCreator();

    @Test
    void createMultipleForks() {
        //given
        List<String> strings = Arrays.asList("a", "b", "c", "d");
        //when
        List<Action> forkWithMaxSize = forkCreator.createForkWithMaxSize(strings, 2);
        //then
        assertThat(forkWithMaxSize, hasSize(2));
        Fork fork1 = (Fork) forkWithMaxSize.get(0);
        assertEquals("fork-0", fork1.name());
        assertThat(fork1.branches(), hasSize(2));

        Fork fork2 = (Fork) forkWithMaxSize.get(1);
        assertEquals("fork-1", fork2.name());
        assertThat(fork2.branches(), hasSize(2));
    }

    @Test
    void createSingleFork() {
        //given
        List<String> strings = Arrays.asList("a", "b", "c");
        //when
        List<Action> forkWithMaxSize = forkCreator.createForkWithMaxSize(strings, 4);
        //then
        assertThat(forkWithMaxSize, hasSize(1));
        Fork fork1 = (Fork) forkWithMaxSize.get(0);
        assertThat(fork1.branches(), hasSize(3));
    }

    @Test
    void throwException() {
        List<String> strings = Arrays.asList("a");
        assertThrows(IllegalArgumentException.class, () -> forkCreator.createForkWithMaxSize(strings, 1));
    }

    @Test
    @DisplayName("не создает форков, если экше всего один")
    void createSingleForkWithOneAction() {
        //given
        List<String> strings = Arrays.asList("a");
        //when
        List<Action> forkWithMaxSize = forkCreator.createForkWithMaxSize(strings, 4);
        //then
        assertThat(forkWithMaxSize, hasSize(2));
        assertTrue(forkWithMaxSize.get(0).name().endsWith(NameAdditions.PROPERTIES_POSTFIX), "первый элемент должен быть properties");
        assertTrue(forkWithMaxSize.get(1) instanceof ActionImpl, "второй элемент должен быть ActionImpl");
    }

    @Test
    @DisplayName("не создает форков, если в последнем форке должен остаться только один экшен")
    void createMultiForkWithOneLastAction() {
        //given
        List<String> strings = Arrays.asList("a", "b", "c");
        //when
        List<Action> forkWithMaxSize = forkCreator.createForkWithMaxSize(strings, 2);
        //then
        assertThat(forkWithMaxSize, hasSize(3));
        assertTrue(forkWithMaxSize.get(0) instanceof Fork, "первый элемент должен быть Fork");
        assertTrue(forkWithMaxSize.get(1).name().endsWith(NameAdditions.PROPERTIES_POSTFIX), "второй элемент должен быть properties");
        assertTrue(forkWithMaxSize.get(2) instanceof ActionImpl, "третий элемент должен быть ActionImpl");
    }
}