package com.example.persistence

import com.example.model.{OrderRow, TransactionRow}
import skunk.codec.all._
import skunk._
import skunk.syntax.all._

import java.time.{Instant, ZoneOffset}

//SQL queries are correct and should not be changed.
object Queries {

  implicit val instantCodec: Codec[Instant] = timestamptz.imap(_.toInstant)(_.atOffset(ZoneOffset.UTC))

  private val marketOrderEncoder: Encoder[TransactionRow] =
    (uuid *: varchar *: numeric *: instantCodec).values.to[TransactionRow]

  private val limitOrderEncoder: Encoder[OrderRow] =
    (varchar *: varchar *: numeric *: numeric *: instantCodec *: instantCodec).values
      .to[OrderRow]

  val insertTransaction: Command[TransactionRow] = {
    sql"""
         INSERT INTO transactions (id, limit_order_id,  amount, created_at)
         VALUES $marketOrderEncoder
       """.command
  }

  val getTransaction: Query[String, TransactionRow] = {
    sql"""
       SELECT * FROM transactions
       WHERE limit_order_id = $text
     """
      .query(uuid ~ text ~ numeric ~ instantCodec)
      .map { case id ~ orderId ~ amount ~ createdAt =>
        TransactionRow(id, orderId, amount, createdAt)
      }
  }

  // could be done better
  val checkTransactionExistence: Query[String *: BigDecimal *: EmptyTuple, Boolean] = sql"""
    SELECT EXISTS (
      SELECT 1 FROM transactions
      WHERE limit_order_id = $text AND amount = $numeric
    )
    """.query(bool)

  val getAllTransactions: Query[Void, TransactionRow] = {
    sql"""
         SELECT * FROM transactions
       """
      .query(uuid ~ text ~ numeric ~ instantCodec)
      .map { case id ~ orderId ~ amount ~ createdAt =>
        TransactionRow(id, orderId, amount, createdAt)
      }
  }

  val getOrder: Query[String, OrderRow] = {
    sql"""
         SELECT * FROM limit_order
         WHERE order_id = $varchar
       """
      .query(text ~ text ~ numeric ~ numeric ~ instantCodec ~ instantCodec)
      .map { case id ~ market ~ total ~ filled ~ created ~ updated =>
        OrderRow(id, market, total, filled, created, updated)
      }
  }

  val getAllOrders: Query[Void, OrderRow] = {
    sql"""
         SELECT * FROM limit_order
       """
      .query(text ~ text ~ numeric ~ numeric ~ instantCodec ~ instantCodec)
      .map { case id ~ market ~ total ~ filled ~ created ~ updated =>
        OrderRow(id, market, total, filled, created, updated)
      }
  }

  val insertOrder: Command[OrderRow] = {
    sql"""
         INSERT INTO limit_order (order_id, market, total, filled, created_at, updated_at)
         VALUES $limitOrderEncoder
       """.command
  }

  val updateOrder: Command[BigDecimal *: String *: EmptyTuple] = {
    sql"""
         UPDATE limit_order
         SET filled = $numeric
         WHERE order_id = $text
       """.command
  }
}
