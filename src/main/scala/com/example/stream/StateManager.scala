package com.example.stream

import cats.effect.{Async, Ref}
import com.example.model.{OrderRow, TransactionRow}
import com.example.persistence.PreparedQueries
import skunk.PreparedCommand
import cats.syntax.all._
import fs2.concurrent.SignallingRef

import java.util.UUID

//Utility for managing placed order state
//This can be used by other components, for example a stream that performs order placement will use add method
final class StateManager[F[_]: Async](
  ioSwitch: SignallingRef[F, Boolean],
  orders: Ref[F, Map[String, OrderRow]],
  transactions: Ref[F, Map[UUID, TransactionRow]]
) {

  def getOrderState(orderId: String): F[Option[OrderRow]] =
    orders.get.map(_.get(orderId))

  def addNewOrderState(order: OrderRow, insert: PreparedCommand[F, OrderRow]): F[Unit] = {
    insert.execute(order).void *>
      orders.update(_ + (order.orderId -> order))
  }

  def updateOrderState(order: OrderRow): F[Unit] =
    getOrderState(order.orderId).map {
      case Some(_) => orders.update(_.updated(order.orderId, order))
      case None    => Async[F].unit // Do nothing if there is no such order
    }

  def getTransactions(orderId: String): F[List[TransactionRow]] =
    transactions.get.map(_.values.toList.filter(_.orderId == orderId))

  def addNewTransaction(transaction: TransactionRow): F[Unit] = {
    transactionExists(transaction.id).map {
      case false =>
        transactions.update(_ + (transaction.id -> transaction))
      case true => Async[F].unit
    }
  }

  def transactionExists(transactionId: UUID): F[Boolean] =
    transactions.get.map(_.contains(transactionId))

  def getSwitch: F[Boolean]              = ioSwitch.get
  def setSwitch(value: Boolean): F[Unit] = ioSwitch.set(value)

  //  def clean(orderId: String): F[Unit] = ???
}

object StateManager {

  def apply[F[_]: Async]: F[StateManager[F]] = for {
    ioSwitch     <- SignallingRef.of(false)
    orders       <- Ref.of[F, Map[String, OrderRow]](Map.empty)
    transactions <- Ref.of[F, Map[UUID, TransactionRow]](Map.empty)
  } yield new StateManager[F](ioSwitch, orders, transactions)

}
