package com.blastme.messaging.dummy;

import com.google.common.util.concurrent.RateLimiter;

public class GoogleGuavaCoba {
	
	

	public GoogleGuavaCoba() {
		
	}
	
	private void doSending() {
		int tps = 5; // 10 transactions per second
		
		// Define rateLimiter
		RateLimiter rateLimiter = RateLimiter.create(tps);
		int count = 0;
		while(true) {
			System.out.println(count + ". Get 5 token: " + rateLimiter.acquire(5) + " second.");
			System.out.println(count + ". Get 1 token: " + rateLimiter.acquire(1) + " second.");
			System.out.println(count + ". Get 1 token: " + rateLimiter.acquire(1) + " second.");
			System.out.println(count + ". Get 1 token: " + rateLimiter.acquire(1) + " second.");
			System.out.println(count + ". Get 1 token: " + rateLimiter.acquire(1) + " second.");
			
			count = count + 1;
			
			
		}
	}

	public static void main(String[] args) {
		GoogleGuavaCoba coba = new GoogleGuavaCoba();
		coba.doSending();
	}

}
