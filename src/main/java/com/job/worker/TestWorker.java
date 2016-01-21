package com.job.worker;

import org.springframework.stereotype.Component;

import com.job.annotation.Worker;

@Worker(topic = "test_worker")
@Component
public class TestWorker extends BaseWorker {

	@Override
	public void handle() throws Exception {

		/**
		 * handle message
		 * */
		System.out.println(topic);
		System.out.println(message);

	}

	@Override
	public String name() {
		return "TestWorker - " + message;
	}
}
