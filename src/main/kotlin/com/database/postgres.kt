package com.database

import com.utils.SnowFlake
import com.gate.logger.Logger
import com.google.common.hash.Hashing
import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import java.sql.SQLException


const val portierePgJdbcUrl = "FIXME"
const val portierePgUser = "FIXME"
const val portierePgPassword = "FIXME"

const val gravitasPgJdbcUrl = "FIXME"
const val gravitasPgUser = "FIXME"
const val gravitasPgPassword = "FIXME"

fun initGravitasDataSource(): HikariDataSource {
    val config = HikariConfig()
    config.driverClassName = "org.postgresql.Driver"
    config.jdbcUrl = gravitasPgJdbcUrl
    config.username = gravitasPgUser
    config.password = gravitasPgPassword
    config.maximumPoolSize = 20
    config.minimumIdle = 10
    return HikariDataSource(config)
}

fun initPortiereDataSource(): HikariDataSource {
    val config = HikariConfig()
    config.driverClassName = "org.postgresql.Driver"
    config.jdbcUrl = portierePgJdbcUrl
    config.username = portierePgUser
    config.password = portierePgPassword
    config.maximumPoolSize = 20
    config.minimumIdle = 10
    return HikariDataSource(config)
}

fun insertGravitasTempAnonymous(
    hds: HikariDataSource,
    snowFlake: SnowFlake,
    chatID: String,
    userID: String,
    nickname: String,
    avatar: String
) {
    try {
        hds.connection.use { con ->
            con.prepareStatement("INSERT INTO temp_anonymous (chat_id, alias_id, user_id, nickname, avatar) VALUES (?, ?, ?, ?, ?)")
                .use { preparedStatement ->
                    preparedStatement.setString(1, chatID)
                    preparedStatement.setLong(2, snowFlake.nextId())
                    preparedStatement.setString(3, userID)
                    preparedStatement.setString(4, nickname)
                    preparedStatement.setString(5, avatar)
                    preparedStatement.executeUpdate()
                }
        }
    } catch (e: SQLException) {
        Logger.error("error, msg=${e.message}")
    }
}

fun getGravitasTempAnonymous(
    hds: HikariDataSource,
    limit: Int,
    offset: Int
): MutableList<UserBrief> {
    val list: MutableList<UserBrief> = mutableListOf()
    try {
        hds.connection.use { con ->
            con.prepareStatement("SELECT chat_id, alias_id, user_id, nickname, avatar FROM temp_anonymous limit ? offset ?")
                .use { prepareStatement ->
                    prepareStatement.setInt(1, limit)
                    prepareStatement.setInt(2, offset)
                    prepareStatement.executeQuery().use { rs ->
                        while (rs.next()) {
                            list.add(
                                UserBrief(
                                    rs.getString("chat_id"),
                                    rs.getLong("alias_id"),
                                    rs.getString("user_id"),
                                    rs.getString("nickname"),
                                    rs.getString("avatar")
                                )
                            )
                        }
                    }
                }
        }
    } catch (e: SQLException) {
        Logger.error("error, msg=${e.message}")
    }
    return list
}

fun insertPortiereAliases(
    hds: HikariDataSource,
    userBrief: UserBrief
) {
    try {
        hds.connection.use { con ->
            con.prepareStatement("INSERT INTO aliases (id, user_id, nickname, avatar, alias_type, external_id, created_time, last_modified_time) VALUES (?, ?, ?, ?, ?, ?, ?, ?)")
                .use { aliasesPS ->
                    con.prepareStatement("SELECT snowflake_user_id FROM users WHERE id = ? limit 1").use { usersPS ->
                        usersPS.setString(1, userBrief.userID)
                        usersPS.executeQuery().use { rs ->
                            var snowflakeUserID = 0L
                            while (rs.next()) {
                                snowflakeUserID = rs.getLong("snowflake_user_id")
                            }
                            aliasesPS.setLong(1, userBrief.aliasID) // id
                            aliasesPS.setLong(2, snowflakeUserID) // user_id
                            aliasesPS.setString(3, userBrief.nickname)
                            aliasesPS.setString(4, userBrief.avatar)
                            when {
                                userBrief.chatID.endsWith("_ggp") -> aliasesPS.setInt(5, 3)
                                userBrief.chatID.endsWith("_sgp") || userBrief.chatID.endsWith("_gp") -> aliasesPS.setInt(5, 2)
                                else -> aliasesPS.setInt(5, 999)
                            } // alias_type
                            when {
                                userBrief.chatID.endsWith("_ggp") -> aliasesPS.setString(6, userBrief.userID)
                                else -> aliasesPS.setString(6, Hashing.sha1().hashString(userBrief.aliasID.toString(), Charsets.US_ASCII).toString().toUpperCase())
                            }
                        } // external_id
                        aliasesPS.setLong(7, 123321) // created_time
                        aliasesPS.setLong(8, System.currentTimeMillis()) // last_modified_time
                        aliasesPS.executeUpdate()
                    }
                }
        }
    } catch (e: SQLException) {
        Logger.error("error, msg=${e.message}")
    }
}

data class UserBrief(
    val chatID: String,
    val aliasID: Long,
    val userID: String,
    val nickname: String,
    val avatar: String
)