package com.jobs

import com.database.initGravitasDataSource
import com.database.insertGravitasTempAnonymous
import com.serializer.RoomParticipantAnonymousData
import com.utils.SnowFlake
import com.datastax.driver.core.Cluster
import com.datastax.driver.core.Session
import com.gate.logger.Logger
import com.zaxxer.hikari.HikariDataSource
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.coroutineScope
import java.lang.Long.max
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicLong

private const val CASSANDRA_HOST = "127.0.0.1"
private const val CASSANDRA_PORT = 9042

suspend fun migrateChatAnonymous() {
    val dataSource: HikariDataSource = initGravitasDataSource()
    dataSource.use { hds ->
        Cluster.builder()
            .addContactPoint(CASSANDRA_HOST)
            .withPort(CASSANDRA_PORT)
            .build().use { cluster ->
                execute(cluster!!.connect(), hds)
            }
    }
}

suspend fun execute(session: Session, hds: HikariDataSource) {
    coroutineScope {
        var tokenValue = Long.MIN_VALUE
        val totalKeyRow = session.execute("SELECT count(*) FROM gate.room_participant_anonymous_data;").one()
        val tmpTokenValue = AtomicLong(tokenValue)
        val insertKeyRow = AtomicInteger(0)
        val insertKeyRowEmpty = AtomicInteger(0)
        val snowFlake = SnowFlake(31, 9) // 1001, 1111101001

        while (true) {
            val rs = session.execute(
                "SELECT token(key), key, value FROM gate.room_participant_anonymous_data WHERE token(key) > ? limit 10000",
                tokenValue
            )

            val rsList = rs.all()
            if (rsList.isEmpty()) break

            rsList.withIndex()
                .groupBy { it.index % 10000 }
                .map { (_, list) ->
                    async(Dispatchers.Default) {
                        list.forEach { row ->
                            val map =
                                RoomParticipantAnonymousData.instance.deserialize(row.value.getBytes("VALUE")) as com.gate.chat.RoomParticipantAnonymousData
                            when {
                                map.data.isNotEmpty() -> insertKeyRow.addAndGet(1)
                                else -> insertKeyRowEmpty.addAndGet(1)
                            }
                            map.data.forEach { (_, userBrief) ->
                                insertGravitasTempAnonymous(
                                    hds,
                                    snowFlake,
                                    map.roomId,
                                    userBrief.id,
                                    userBrief.nickname,
                                    userBrief.photo
                                )
                            }
                            tmpTokenValue.set(max(row.value.getLong("token(key)"), tmpTokenValue.get()))
                        }
                    }
                }.awaitAll()

            tokenValue = tmpTokenValue.get()
            Logger.debug("tokenValue = $tokenValue")
        }
        Logger.info("totalKeyRow = $totalKeyRow, insertKeyRow = ${insertKeyRow.get()}, insertKeyRowEmpty = ${insertKeyRowEmpty.get()}")
    }
}