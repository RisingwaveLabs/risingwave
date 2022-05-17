package com.risingwave.pgwire

import com.risingwave.pgwire.database.DatabaseManager
import io.ktor.network.selector.ActorSelectorManager
import io.ktor.network.sockets.ServerSocket
import io.ktor.network.sockets.Socket
import io.ktor.network.sockets.aSocket
import io.ktor.network.sockets.isClosed
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import org.slf4j.LoggerFactory
import java.net.InetSocketAddress

class PgServer(private val ip: String, private val port: Int, private val dbManager: DatabaseManager) {
  companion object {
    private val log = LoggerFactory.getLogger(PgServer::class.java)
  }

  private lateinit var acceptor: ServerSocket

  fun serve() {
    runBlocking { // The coroutine scope.
      val addr = InetSocketAddress(ip, port)
      acceptor = aSocket(ActorSelectorManager(Dispatchers.Default)).tcp().bind(addr)
      log.info("Started server at ${acceptor.localAddress}")

      // This loop only terminates due to kill signals.
      // Single connection failure won't break it.
      while (!acceptor.isClosed) {
        val socket: Socket = acceptor.accept()
        val conn = PgServerConn(socket, dbManager)
        launch { // Spawn a separate coroutine handling this connection.
          conn.serve()
        }
      }
    }
  }

  fun close() {
    this.acceptor.close()
  }
}
