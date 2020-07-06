package com.jobs

import com.database.getGravitasTempAnonymous
import com.database.initGravitasDataSource
import com.database.initPortiereDataSource
import com.database.insertPortiereAliases
import com.gate.logger.Logger
import com.zaxxer.hikari.HikariDataSource
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.coroutineScope

suspend fun migrateUserAliases() {
    val queryLimit = 10000
    initGravitasDataSource().use { gravitasHds ->
        initPortiereDataSource().use { portiereHds ->
            execute(gravitasHds, portiereHds, queryLimit)
        }
    }
}

private suspend fun execute(
    gravitasHds: HikariDataSource,
    portiereHds: HikariDataSource,
    queryLimit: Int
) {
    coroutineScope {
        var queryOffset = 0
        while (true) {
            val rsList = getGravitasTempAnonymous(gravitasHds, queryLimit, queryOffset)
            if (rsList.isEmpty()) break

            rsList.withIndex()
                .groupBy { it.index % 10000 }
                .map { (_, list) ->
                    async(Dispatchers.Default) {
                        list.forEach { userBrief ->
                            insertPortiereAliases(portiereHds, userBrief.value)
                        }
                    }
                }.awaitAll()

            queryOffset += queryLimit
            Logger.debug("queryOffset = $queryOffset")
        }
    }
}
