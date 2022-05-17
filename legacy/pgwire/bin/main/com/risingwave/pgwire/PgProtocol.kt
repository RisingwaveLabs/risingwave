package com.risingwave.pgwire

import com.risingwave.common.exception.PgErrorCode
import com.risingwave.common.exception.PgException
import com.risingwave.pgwire.database.DatabaseManager
import com.risingwave.pgwire.msg.Messages
import com.risingwave.pgwire.msg.PgMessage
import com.risingwave.pgwire.msg.PgMsgType
import io.grpc.StatusRuntimeException
import io.ktor.utils.io.ByteReadChannel
import io.ktor.utils.io.ByteWriteChannel
import kotlinx.coroutines.channels.ClosedReceiveChannelException
import org.slf4j.LoggerFactory
import java.io.IOException
import java.nio.ByteBuffer

class PgProtocol(
  private val input: ByteReadChannel,
  private val output: ByteWriteChannel,
  private val dbManager: DatabaseManager
) {
  companion object {
    private val log = LoggerFactory.getLogger(PgServerConn::class.java)
  }

  private var startedUp: Boolean = false
  private var stm: StateMachine = StateMachine(output, dbManager)

  /** Process one client message and reply with response if any.
   *  @return true if to terminate the connection.
   */
  suspend fun process(): Boolean {
    try {
      if (doProcess()) {
        return true
      }
      return stm.willTerminate
    } catch (err: PgException) {
      log.error("Failed to process.", err)
      stm.processPgError(err)
      return false
    }
  }

  @Throws(PgException::class)
  private suspend fun doProcess(): Boolean {
    try {
      val msg = readMessage()
      stm.process(msg)
      if (msg.type == PgMsgType.StartupMessage) {
        startedUp = true
      }
    } catch (exp: IOException) {
      throw PgException(PgErrorCode.CONNECTION_EXCEPTION, exp)
    } catch (exp: StatusRuntimeException) {
      throw PgException.fromGrpcException(exp)
    } catch (exp: PgException) {
      throw exp
    } catch (exp: ClosedReceiveChannelException) {
      return true
    } catch (exp: Throwable) {
      throw PgException(PgErrorCode.INTERNAL_ERROR, exp)
    }
    return false
  }

  private suspend fun readMessage(): PgMessage {
    if (!startedUp) {
      return readStartupPacket()
    }
    return readRegularPacket()
  }

  // Regular Packet
  // +----------+-----------+---------+
  // | char tag | int32 len | payload |
  // +----------+-----------+---------+
  private suspend fun readRegularPacket(): PgMessage {
    val tag = input.readByte()
    val len = input.readInt()

    val payload = ByteBuffer.allocate(len - 4)
    if (len - 4 > 0) {
      input.readFully(payload)
    }
    return Messages.createRegularPacket(tag, payload.array())
  }

  // Startup Packet
  // +-----------+----------------+---------+
  // | int32 len | int32 protocol | payload |
  // +-----------+----------------+---------+
  private suspend fun readStartupPacket(): PgMessage {
    val len = input.readInt()
    val protocol = input.readInt()

    val payload = ByteBuffer.allocate(len - 8)
    if (len - 8 > 0) {
      input.readFully(payload)
    }
    return Messages.createStartupPacket(protocol, payload.array())
  }
}
