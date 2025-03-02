package org.example;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Calendar;
import java.util.Random;

public class ClickSource implements SourceFunction<Event>{
    private Boolean running =true;

    public void run(SourceFunction.SourceContext<Event> sourceContext) throws InterruptedException {
        Random random = new Random();
        String[] name={"apple","banana","pie","APPLE","BANANA","PIE"};

        while (running){
            sourceContext.collect(new Event(
                    name[random.nextInt(name.length)],
                    Calendar.getInstance().getTimeInMillis()
            ));
            Thread.sleep(1000);
        }
    }

    @Override
    public void cancel() {
        running = false;
    }
}
