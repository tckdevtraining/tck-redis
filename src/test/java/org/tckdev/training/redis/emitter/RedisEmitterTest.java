package org.tckdev.training.redis.emitter;

import static org.junit.jupiter.api.Assertions.*;

import java.util.List;

import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.junit.BaseComponentsHandler;
import org.talend.sdk.component.junit.environment.Environment;
import org.talend.sdk.component.junit.environment.EnvironmentConfiguration;
import org.talend.sdk.component.junit.environment.builtin.ContextualEnvironment;
import org.talend.sdk.component.junit5.Injected;
import org.talend.sdk.component.junit5.WithComponents;
import org.talend.sdk.component.junit5.environment.EnvironmentalTest;
import org.talend.sdk.component.runtime.manager.chain.Job;


@Environment(ContextualEnvironment.class)
@EnvironmentConfiguration(environment = "Contextual", systemProperties = {})

@WithComponents(value = "org.tckdev.training.redis")
class RedisEmitterTest {

    @Injected
    private BaseComponentsHandler handler;


    @EnvironmentalTest
    void testInput() {
        Job.components() //
                .component("emitter", "Redis://RedisEmitter") //
                .component("out", "test://collector") //
                .connections() //
                .from("emitter") //
                .to("out") //
                .build() //
                .run();

        final List<Record> records = handler.getCollectedData(Record.class);

        assertEquals(1, records.size());
        assertEquals("my_value", records.get(0).getString("my_key"));
    }

}