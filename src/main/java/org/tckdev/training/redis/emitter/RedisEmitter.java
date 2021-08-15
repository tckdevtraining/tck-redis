package org.tckdev.training.redis.emitter;

import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;

import java.io.Serializable;

import javax.annotation.PostConstruct;

import org.talend.sdk.component.api.component.Icon;
import org.talend.sdk.component.api.component.Version;
import org.talend.sdk.component.api.input.Emitter;
import org.talend.sdk.component.api.input.Producer;
import org.talend.sdk.component.api.meta.Documentation;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.record.Record.Builder;
import org.talend.sdk.component.api.service.record.RecordBuilderFactory;

@Version(value = 1)
@Icon(value = Icon.IconType.CUSTOM, custom = "redis-logo")
@Emitter(name = "RedisEmitter")
@Documentation("A Redis TCK emitter.")
public class RedisEmitter implements Serializable {

    private boolean done = false;
    private RedisCommands<String, String> syncCommands;
    final RecordBuilderFactory recordBuilderFactory;


    public RedisEmitter(final RecordBuilderFactory recordBuilderFactory){
        this.recordBuilderFactory = recordBuilderFactory;
    }

    @PostConstruct
    public void init() {
        RedisClient redisClient = RedisClient.create("redis://default:aze123_@localhost:6379/0");
        StatefulRedisConnection<String, String> connection = redisClient.connect();
        syncCommands = connection.sync();
    }

    @Producer
    public Record next() {
        if(done){
            return null;
        }

        final String my_key_value = syncCommands.get("my_key");

        final Builder builder = recordBuilderFactory.newRecordBuilder();
        builder.withString("my_key", my_key_value);

        done = true;

        return builder.build();
    }

}