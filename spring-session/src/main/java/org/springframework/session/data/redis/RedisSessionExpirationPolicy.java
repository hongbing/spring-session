/*
 * Copyright 2014-2016 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.session.data.redis;

import java.util.Calendar;
import java.util.Date;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.springframework.data.redis.core.BoundSetOperations;
import org.springframework.data.redis.core.RedisOperations;
import org.springframework.session.ExpiringSession;
import org.springframework.session.data.redis.RedisOperationsSessionRepository.RedisSession;

/**
 * A strategy for expiring {@link RedisSession} instances. This performs two operations:
 *
 * Redis has no guarantees of when an expired session event will be fired. In order to
 * ensure expired session events are processed in a timely fashion the expiration (rounded
 * to the nearest minute) is mapped to all the sessions that expire at that time. Whenever
 * {@link #cleanExpiredSessions()} is invoked, the sessions for the previous minute are
 * then accessed to ensure they are deleted if expired.
 *
 * In some instances the {@link #cleanExpiredSessions()} method may not be not invoked for
 * a specific time. For example, this may happen when a server is restarted. To account
 * for this, the expiration on the Redis session is also set.
 *
 * @author Rob Winch
 * @since 1.0
 */
final class RedisSessionExpirationPolicy {

	private static final Log logger = LogFactory
			.getLog(RedisSessionExpirationPolicy.class);

	private final RedisOperations<Object, Object> redis;

	private final RedisOperationsSessionRepository redisSession;

	RedisSessionExpirationPolicy(RedisOperations<Object, Object> sessionRedisOperations,
			RedisOperationsSessionRepository redisSession) {
		super();
		this.redis = sessionRedisOperations;
		this.redisSession = redisSession;
	}

	public void onDelete(ExpiringSession session) {
		long toExpire = roundUpToNextMinute(expiresInMillis(session));
		String expireKey = getExpirationKey(toExpire);
		this.redis.boundSetOps(expireKey).remove(session.getId());
	}

	/**
	 * 更新session的过期数据
	 * @param originalExpirationTimeInMilli 上一次过期时间
	 * @param session
	 */
	public void onExpirationUpdated(Long originalExpirationTimeInMilli,
			ExpiringSession session) {
		String keyToExpire = "expires:" + session.getId();
		// session被访问后将要过期的下一分钟
		long toExpire = roundUpToNextMinute(expiresInMillis(session));

		if (originalExpirationTimeInMilli != null) {
			long originalRoundedUp = roundUpToNextMinute(originalExpirationTimeInMilli);
			// 更新expirations:[min],前后两次过期时间不在同一分钟内，将前一个过期时间内的expires key删除
			if (toExpire != originalRoundedUp) {
				String expireKey = getExpirationKey(originalRoundedUp);
				this.redis.boundSetOps(expireKey).remove(keyToExpire);
			}
		}

		long sessionExpireInSeconds = session.getMaxInactiveIntervalInSeconds();
		String sessionKey = getSessionKey(keyToExpire);

		if (sessionExpireInSeconds < 0) {
			this.redis.boundValueOps(sessionKey).append("");
			this.redis.boundValueOps(sessionKey).persist();
			this.redis.boundHashOps(getSessionKey(session.getId())).persist();
			return;
		}

		// 设置下一次过期时间的expirations key
		String expireKey = getExpirationKey(toExpire);
		BoundSetOperations<Object, Object> expireOperations = this.redis
				.boundSetOps(expireKey);
		expireOperations.add(keyToExpire);

		long fiveMinutesAfterExpires = sessionExpireInSeconds
				+ TimeUnit.MINUTES.toSeconds(5);

		// expirations:[min] key的过期时间加5分钟
		expireOperations.expire(fiveMinutesAfterExpires, TimeUnit.SECONDS);
		if (sessionExpireInSeconds == 0) {
			this.redis.delete(sessionKey);
		}
		else {
			// expires:[sessionId] 值为“”，过期时间为MaxInactiveIntervalInSeconds
			this.redis.boundValueOps(sessionKey).append("");
			this.redis.boundValueOps(sessionKey).expire(sessionExpireInSeconds,
					TimeUnit.SECONDS);
		}
		// sessions:[sessionId]的过期时间 加5分钟
		this.redis.boundHashOps(getSessionKey(session.getId()))
				.expire(fiveMinutesAfterExpires, TimeUnit.SECONDS);
	}

	// spring:session:expirations:[expiresTime]
	String getExpirationKey(long expires) {
		return this.redisSession.getExpirationsKey(expires);
	}

	// spring:session:sessions:[sessionId]
	String getSessionKey(String sessionId) {
		return this.redisSession.getSessionKey(sessionId);
	}

	public void cleanExpiredSessions() {
		long now = System.currentTimeMillis();
		long prevMin = roundDownMinute(now);

		if (logger.isDebugEnabled()) {
			logger.debug("Cleaning up sessions expiring at " + new Date(prevMin));
		}
		// preMin时间到，将spring:session:expirations:[min], set集合中members包括了这一分钟之内需要过期的所有
		// expire key删掉, member元素为expires:[sessionId]
		String expirationKey = getExpirationKey(prevMin);
		Set<Object> sessionsToExpire = this.redis.boundSetOps(expirationKey).members();
		this.redis.delete(expirationKey);
		for (Object session : sessionsToExpire) {
			// sessionKey为spring:session:sessions:expires:[sessionId]
			String sessionKey = getSessionKey((String) session);
			//利用redis的惰性删除策略
			touch(sessionKey);
		}
	}

	/**
	 * By trying to access the session we only trigger a deletion if it the TTL is
	 * expired. This is done to handle
	 * https://github.com/spring-projects/spring-session/issues/93
	 *
	 * @param key the key
	 */
	private void touch(String key) {
		this.redis.hasKey(key);
	}

	static long expiresInMillis(ExpiringSession session) {
		int maxInactiveInSeconds = session.getMaxInactiveIntervalInSeconds();
		long lastAccessedTimeInMillis = session.getLastAccessedTime();
		return lastAccessedTimeInMillis + TimeUnit.SECONDS.toMillis(maxInactiveInSeconds);
	}

	// 下一分钟的ms数
	static long roundUpToNextMinute(long timeInMs) {

		Calendar date = Calendar.getInstance();
		date.setTimeInMillis(timeInMs);
		date.add(Calendar.MINUTE, 1);
		date.clear(Calendar.SECOND);
		date.clear(Calendar.MILLISECOND);
		return date.getTimeInMillis();
	}

	// 向下取整的那一分钟对应的ms数
	static long roundDownMinute(long timeInMs) {
		Calendar date = Calendar.getInstance();
		date.setTimeInMillis(timeInMs);
		date.clear(Calendar.SECOND);
		date.clear(Calendar.MILLISECOND);
		return date.getTimeInMillis();
	}
}
